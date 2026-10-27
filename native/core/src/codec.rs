//! Byte-level codecs.  The numeric ids are part of the DCZ file format and must never change.

use std::io::{Read, Write};

use crate::error::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CodecId {
    None = 0,
    Zlib = 1,
    // 2 is reserved (bz2) and never written.
    Lzma = 3,
    Zstd = 4,
    Lz4 = 5,
    Brotli = 6,
}

impl CodecId {
    pub fn from_u8(v: u8) -> Result<Self> {
        Ok(match v {
            0 => CodecId::None,
            1 => CodecId::Zlib,
            3 => CodecId::Lzma,
            4 => CodecId::Zstd,
            5 => CodecId::Lz4,
            6 => CodecId::Brotli,
            other => return Err(Error::Format(format!("unknown codec id {other}"))),
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            CodecId::None => "none",
            CodecId::Zlib => "zlib",
            CodecId::Lzma => "lzma",
            CodecId::Zstd => "zstd",
            CodecId::Lz4 => "lz4",
            CodecId::Brotli => "brotli",
        }
    }

    pub fn parse(name: &str) -> Result<Self> {
        Ok(match name.to_ascii_lowercase().as_str() {
            "none" | "raw" | "store" | "uncompressed" => CodecId::None,
            "zlib" | "deflate" | "gzip" | "gz" => CodecId::Zlib,
            "lzma" | "xz" => CodecId::Lzma,
            "zstd" | "zst" | "zstandard" => CodecId::Zstd,
            "lz4" => CodecId::Lz4,
            "brotli" | "br" => CodecId::Brotli,
            other => return Err(Error::Invalid(format!("unknown codec '{other}'"))),
        })
    }

    /// Clamp a requested level to the codec's valid range.
    pub fn clamp_level(self, level: i32) -> i32 {
        match self {
            CodecId::None | CodecId::Lz4 => 0,
            CodecId::Zlib => level.clamp(0, 9),
            CodecId::Lzma => level.clamp(0, 9),
            CodecId::Zstd => level.clamp(-7, 22),
            CodecId::Brotli => level.clamp(0, 11),
        }
    }
}

/// Compression effort presets shared by every codec.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    Fast,
    Balanced,
    Max,
}

impl Profile {
    pub fn parse(name: &str) -> Result<Self> {
        Ok(match name.to_ascii_lowercase().as_str() {
            "fast" | "speed" => Profile::Fast,
            "balanced" | "default" | "auto" => Profile::Balanced,
            "max" | "best" | "size" | "small" => Profile::Max,
            other => return Err(Error::Invalid(format!("unknown profile '{other}' (fast, balanced, max)"))),
        })
    }

    pub fn level_for(self, codec: CodecId) -> i32 {
        match (self, codec) {
            (_, CodecId::None) | (_, CodecId::Lz4) => 0,
            (Profile::Fast, CodecId::Zstd) => 1,
            (Profile::Balanced, CodecId::Zstd) => 9,
            (Profile::Max, CodecId::Zstd) => 22,
            (Profile::Fast, CodecId::Zlib) => 1,
            (Profile::Balanced, CodecId::Zlib) => 6,
            (Profile::Max, CodecId::Zlib) => 9,
            (Profile::Fast, CodecId::Lzma) => 1,
            (Profile::Balanced, CodecId::Lzma) => 6,
            (Profile::Max, CodecId::Lzma) => 9,
            (Profile::Fast, CodecId::Brotli) => 2,
            (Profile::Balanced, CodecId::Brotli) => 7,
            (Profile::Max, CodecId::Brotli) => 11,
        }
    }
}

/// Compress `data`.  For lzma, level 9 additionally turns on the "extreme" preset.
pub fn compress(codec: CodecId, data: &[u8], level: i32) -> Result<Vec<u8>> {
    let level = codec.clamp_level(level);
    match codec {
        CodecId::None => Ok(data.to_vec()),
        CodecId::Zstd => zstd::bulk::compress(data, level).map_err(Error::from),
        CodecId::Zlib => {
            let mut enc = flate2::write::ZlibEncoder::new(
                Vec::with_capacity(data.len() / 2 + 64),
                flate2::Compression::new(level as u32),
            );
            enc.write_all(data)?;
            Ok(enc.finish()?)
        }
        CodecId::Lz4 => {
            let mut enc = lz4_flex::frame::FrameEncoder::new(Vec::with_capacity(data.len() / 2 + 64));
            enc.write_all(data)?;
            enc.finish().map_err(|e| Error::Data(format!("lz4: {e}")))
        }
        CodecId::Brotli => {
            let mut out = Vec::with_capacity(data.len() / 2 + 64);
            let params = brotli::enc::BrotliEncoderParams {
                quality: level,
                lgwin: brotli_window(data.len()),
                size_hint: data.len(),
                ..Default::default()
            };
            let mut input = data;
            brotli::BrotliCompress(&mut input, &mut out, &params)?;
            Ok(out)
        }
        CodecId::Lzma => lzma_alone_compress(data, level as u32),
    }
}

/// Largest up-front reservation for a decoded length read from a file.  Bigger
/// outputs grow as they are produced, so a corrupt length cannot reserve memory the
/// data does not really expand to.
const RESERVE_CAP: usize = 256 << 20;

/// An output buffer for `len` bytes, where `len` came from untrusted input: running
/// out of memory is an error instead of an abort.
pub fn buffer(len: usize) -> Result<Vec<u8>> {
    let mut v = Vec::new();
    v.try_reserve_exact(len.min(RESERVE_CAP))
        .map_err(|_| Error::Data(format!("not enough memory to decode {len} bytes")))?;
    Ok(v)
}

/// Read a decoder to the end, stopping one byte past `raw_len` (enough to prove a
/// wrong length without inflating a decompression bomb).
fn read_limited(reader: impl Read, raw_len: usize, name: &str) -> Result<Vec<u8>> {
    let mut out = buffer(raw_len)?;
    reader.take(raw_len as u64 + 1).read_to_end(&mut out).map_err(|e| Error::Integrity(format!("{name}: {e}")))?;
    Ok(out)
}

/// Decompress `data`, which must expand to exactly `raw_len` bytes.
pub fn decompress(codec: CodecId, data: &[u8], raw_len: usize) -> Result<Vec<u8>> {
    let out = match codec {
        CodecId::None => data.to_vec(),
        CodecId::Zstd if raw_len == 0 => Vec::new(),
        CodecId::Zstd if raw_len <= RESERVE_CAP => {
            // bulk decoding into a buffer of exactly raw_len fails on larger output
            let mut out = buffer(raw_len)?;
            zstd::bulk::Decompressor::new()
                .and_then(|mut d| d.decompress_to_buffer(data, &mut out))
                .map_err(|e| Error::Integrity(format!("zstd: {e}")))?;
            out
        }
        CodecId::Zstd => read_limited(
            zstd::stream::read::Decoder::with_buffer(data).map_err(|e| Error::Integrity(format!("zstd: {e}")))?,
            raw_len,
            "zstd",
        )?,
        CodecId::Zlib => read_limited(flate2::read::ZlibDecoder::new(data), raw_len, "zlib")?,
        CodecId::Lz4 => read_limited(lz4_flex::frame::FrameDecoder::new(data), raw_len, "lz4")?,
        CodecId::Brotli => read_limited(brotli::Decompressor::new(data, 1 << 16), raw_len, "brotli")?,
        CodecId::Lzma => lzma_alone_decompress(data, raw_len)?,
    };
    if out.len() != raw_len {
        return Err(Error::Integrity(format!(
            "{} stream expanded to {} bytes, expected {}",
            codec.name(),
            out.len(),
            raw_len
        )));
    }
    Ok(out)
}

fn brotli_window(len: usize) -> i32 {
    let mut bits = 16;
    while bits < 24 && (1usize << bits) < len {
        bits += 1;
    }
    bits
}

/// Dictionary size: the smallest power of two covering the input (bounded), so small
/// inputs never pay for a 64 MiB window.
fn lzma_dict_size(len: usize) -> u32 {
    let mut size: u32 = 1 << 16;
    while size < (1 << 26) && (size as usize) < len {
        size <<= 1;
    }
    size
}

/// The legacy ".lzma" (LZMA_Alone) container: a 13-byte header, no checksum.  Python reads
/// it with `lzma.decompress(data, format=lzma.FORMAT_ALONE)`.
fn lzma_alone_compress(data: &[u8], level: u32) -> Result<Vec<u8>> {
    use liblzma::stream::{LzmaOptions, Stream};
    let preset = if level >= 9 { 9 | liblzma::stream::PRESET_EXTREME } else { level };
    let mut opts = LzmaOptions::new_preset(preset).map_err(|e| Error::Invalid(format!("lzma: {e}")))?;
    opts.dict_size(lzma_dict_size(data.len()));
    let stream = Stream::new_lzma_encoder(&opts).map_err(|e| Error::Invalid(format!("lzma: {e}")))?;
    let mut enc = liblzma::write::XzEncoder::new_stream(Vec::with_capacity(data.len() / 3 + 64), stream);
    enc.write_all(data)?;
    Ok(enc.finish()?)
}

fn lzma_alone_decompress(data: &[u8], raw_len: usize) -> Result<Vec<u8>> {
    use liblzma::stream::Stream;
    // The header names the dictionary size; the encoder never uses more than 64 MiB,
    // so a corrupt header cannot make the decoder allocate gigabytes.
    let stream = Stream::new_lzma_decoder(1 << 30).map_err(|e| Error::Integrity(format!("lzma: {e}")))?;
    read_limited(liblzma::read::XzDecoder::new_stream(data, stream), raw_len, "lzma")
}

/// Compress with each candidate and keep the smallest result (ties prefer the earlier,
/// faster-to-decode candidate).  Falls back to storing raw bytes when nothing helps.
pub fn compress_best(candidates: &[(CodecId, i32)], data: &[u8]) -> Result<(CodecId, Vec<u8>)> {
    let mut best: Option<(CodecId, Vec<u8>)> = None;
    for &(codec, level) in candidates {
        let out = compress(codec, data, level)?;
        if best.as_ref().is_none_or(|(_, b)| out.len() < b.len()) {
            best = Some((codec, out));
        }
    }
    match best {
        Some((codec, out)) if out.len() < data.len() => Ok((codec, out)),
        _ => Ok((CodecId::None, data.to_vec())),
    }
}

/// Multi-threaded zstd for large raw inputs (used by the "fast" raw compression path).
pub fn zstd_compress_mt(data: &[u8], level: i32, threads: u32) -> Result<Vec<u8>> {
    let mut enc = zstd::stream::Encoder::new(Vec::with_capacity(data.len() / 3 + 64), level)?;
    if threads > 1 {
        enc.multithread(threads)?;
    }
    enc.include_contentsize(true)?;
    enc.set_pledged_src_size(Some(data.len() as u64))?;
    enc.write_all(data)?;
    Ok(enc.finish()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_all_codecs() {
        let mut data = Vec::new();
        for i in 0..20000u32 {
            data.extend_from_slice(format!("{},{},row{}\n", i, i * 7 % 13, i % 100).as_bytes());
        }
        for codec in [CodecId::None, CodecId::Zlib, CodecId::Lzma, CodecId::Zstd, CodecId::Lz4, CodecId::Brotli] {
            for profile in [Profile::Fast, Profile::Balanced, Profile::Max] {
                let level = profile.level_for(codec);
                let packed = compress(codec, &data, level).unwrap();
                let back = decompress(codec, &packed, data.len()).unwrap();
                assert_eq!(back, data, "{codec:?} {profile:?}");
            }
            let empty = compress(codec, b"", 3).unwrap();
            assert_eq!(decompress(codec, &empty, 0).unwrap(), b"");
        }
    }

    #[test]
    fn wrong_length_is_integrity_error() {
        let packed = compress(CodecId::Zlib, b"hello world", 6).unwrap();
        assert!(matches!(decompress(CodecId::Zlib, &packed, 3), Err(Error::Integrity(_))));
    }

    #[test]
    fn best_picks_smallest() {
        let data = vec![7u8; 100_000];
        let (codec, out) = compress_best(&[(CodecId::Lz4, 0), (CodecId::Zstd, 19)], &data).unwrap();
        assert_eq!(codec, CodecId::Zstd);
        assert!(out.len() < 100);
        let (codec, _) = compress_best(&[(CodecId::Zstd, 3)], b"x").unwrap();
        assert_eq!(codec, CodecId::None);
    }
}
