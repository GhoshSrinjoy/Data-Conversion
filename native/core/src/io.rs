//! File I/O: transparent decompression, transcoding to UTF-8, progress counting and
//! atomic outputs (a failed or interrupted job never leaves a truncated file behind).

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    None,
    Gzip,
    Zstd,
    Xz,
    Lz4,
    Brotli,
    Bzip2,
    Zip,
}

impl Compression {
    pub fn name(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Gzip => "gzip",
            Compression::Zstd => "zstd",
            Compression::Xz => "xz",
            Compression::Lz4 => "lz4",
            Compression::Brotli => "brotli",
            Compression::Bzip2 => "bz2",
            Compression::Zip => "zip",
        }
    }

    pub fn from_extension(path: &Path) -> Compression {
        match path.extension().and_then(|e| e.to_str()).map(|e| e.to_ascii_lowercase()).as_deref() {
            Some("gz") | Some("gzip") => Compression::Gzip,
            Some("zst") | Some("zstd") => Compression::Zstd,
            Some("xz") | Some("lzma") => Compression::Xz,
            Some("lz4") => Compression::Lz4,
            Some("br") => Compression::Brotli,
            Some("bz2") => Compression::Bzip2,
            Some("zip") => Compression::Zip,
            _ => Compression::None,
        }
    }
}

/// Sniff a compression wrapper from magic bytes, falling back to the extension
/// (brotli has no magic number).
pub fn detect_compression(path: &Path, head: &[u8]) -> Compression {
    if head.starts_with(&[0x1f, 0x8b]) {
        Compression::Gzip
    } else if head.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        Compression::Zstd
    } else if head.starts_with(&[0xfd, b'7', b'z', b'X', b'Z', 0x00]) {
        Compression::Xz
    } else if head.starts_with(&[0x04, 0x22, 0x4d, 0x18]) {
        Compression::Lz4
    } else if head.starts_with(b"BZh") {
        Compression::Bzip2
    } else if head.starts_with(b"PK\x03\x04") {
        Compression::Zip
    } else if Compression::from_extension(path) == Compression::Brotli {
        Compression::Brotli
    } else {
        Compression::None
    }
}

pub fn read_head(path: &Path, n: usize) -> Result<Vec<u8>> {
    if path.is_dir() {
        return Err(Error::Invalid(format!("{} is a directory, not a file", path.display())));
    }
    let mut file = File::open(path).map_err(|e| Error::io(e, path))?;
    let mut buf = vec![0u8; n];
    let mut filled = 0;
    while filled < n {
        let got = file.read(&mut buf[filled..]).map_err(|e| Error::io(e, path))?;
        if got == 0 {
            break;
        }
        filled += got;
    }
    buf.truncate(filled);
    Ok(buf)
}

/// Counts bytes pulled from the underlying (possibly compressed) file, for progress.
pub struct CountingReader<R> {
    inner: R,
    count: Arc<AtomicU64>,
}

impl<R> CountingReader<R> {
    pub fn new(inner: R, count: Arc<AtomicU64>) -> Self {
        CountingReader { inner, count }
    }
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.count.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

pub struct Input {
    pub reader: Box<dyn Read + Send>,
    pub compression: Compression,
    /// Size of the file on disk (compressed size for compressed inputs).
    pub file_size: u64,
    /// Bytes of the on-disk file consumed so far.
    pub consumed: Arc<AtomicU64>,
}

fn open_counted(path: &Path) -> Result<(BufReader<CountingReader<File>>, u64, Arc<AtomicU64>)> {
    if path.is_dir() {
        return Err(Error::Invalid(format!("{} is a directory, not a file", path.display())));
    }
    let file = File::open(path).map_err(|e| Error::io(e, path))?;
    let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);
    let consumed = Arc::new(AtomicU64::new(0));
    let counted = BufReader::with_capacity(1 << 20, CountingReader { inner: file, count: consumed.clone() });
    Ok((counted, file_size, consumed))
}

/// Whether two paths name the same existing file.
pub fn same_file(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(x), Ok(y)) => x == y,
        _ => false,
    }
}

/// Open `path` for streaming its bytes as they are (no decompression).
pub fn open_raw(path: &Path) -> Result<Input> {
    let (counted, file_size, consumed) = open_counted(path)?;
    Ok(Input { reader: Box::new(counted), compression: Compression::None, file_size, consumed })
}

/// Decodes concatenated LZ4 frames: the frame decoder reports the end of the data
/// at the end of each frame.
struct Lz4Frames<R: io::BufRead> {
    decoder: Option<lz4_flex::frame::FrameDecoder<R>>,
}

impl<R: io::BufRead> Read for Lz4Frames<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let n = self.decoder.as_mut().expect("decoder").read(buf)?;
            if n > 0 || buf.is_empty() {
                return Ok(n);
            }
            // the end of a frame: another one may follow
            let mut inner = self.decoder.take().expect("decoder").into_inner();
            let more = !inner.fill_buf()?.is_empty();
            self.decoder = Some(lz4_flex::frame::FrameDecoder::new(inner));
            if !more {
                return Ok(0);
            }
        }
    }
}

/// Open `path` for streaming, decompressing transparently.
pub fn open_input(path: &Path) -> Result<Input> {
    let (counted, file_size, consumed) = open_counted(path)?;
    let head = read_head(path, 16)?;
    let compression = detect_compression(path, &head);
    let reader: Box<dyn Read + Send> = match compression {
        Compression::None => Box::new(counted),
        Compression::Gzip => Box::new(flate2::read::MultiGzDecoder::new(counted)),
        Compression::Zstd => Box::new(zstd::stream::read::Decoder::with_buffer(counted)?),
        Compression::Xz => Box::new(liblzma::read::XzDecoder::new_multi_decoder(counted)),
        Compression::Lz4 => Box::new(Lz4Frames { decoder: Some(lz4_flex::frame::FrameDecoder::new(counted)) }),
        Compression::Brotli => Box::new(brotli::Decompressor::new(counted, 1 << 16)),
        Compression::Bzip2 | Compression::Zip => {
            return Err(Error::Format(format!(
                "{} input is not supported by the native engine: {}",
                compression.name(),
                path.display()
            )))
        }
    };
    Ok(Input { reader, compression, file_size, consumed })
}

/// Read up to `n` bytes (fewer only at end of stream).
pub fn read_up_to<R: Read + ?Sized>(reader: &mut R, n: usize) -> io::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(n.min(1 << 26));
    reader.take(n as u64).read_to_end(&mut buf)?;
    Ok(buf)
}

/// Streaming transcoder from any WHATWG encoding to UTF-8 (BOMs are stripped).
pub struct Utf8Reader<R> {
    inner: R,
    decoder: encoding_rs::Decoder,
    in_buf: Vec<u8>,
    out_buf: Vec<u8>,
    out_pos: usize,
    eof: bool,
}

impl<R: Read> Utf8Reader<R> {
    pub fn new(inner: R, encoding: &'static encoding_rs::Encoding) -> Self {
        Utf8Reader {
            inner,
            decoder: encoding.new_decoder_with_bom_removal(),
            in_buf: vec![0u8; 1 << 16],
            out_buf: Vec::new(),
            out_pos: 0,
            eof: false,
        }
    }
}

impl<R: Read> Read for Utf8Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.out_pos < self.out_buf.len() {
                let n = (self.out_buf.len() - self.out_pos).min(buf.len());
                buf[..n].copy_from_slice(&self.out_buf[self.out_pos..self.out_pos + n]);
                self.out_pos += n;
                return Ok(n);
            }
            if self.eof {
                return Ok(0);
            }
            let got = self.inner.read(&mut self.in_buf)?;
            let last = got == 0;
            self.out_buf.clear();
            self.out_pos = 0;
            let cap = self.decoder.max_utf8_buffer_length(got).unwrap_or(got * 3 + 16);
            self.out_buf.resize(cap, 0);
            let (result, _read, written, had_errors) =
                self.decoder.decode_to_utf8(&self.in_buf[..got], &mut self.out_buf, last);
            debug_assert!(matches!(result, encoding_rs::CoderResult::InputEmpty));
            self.out_buf.truncate(written);
            if had_errors {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid byte sequence for the input encoding"));
            }
            if last {
                self.eof = true;
            }
        }
    }
}

/// Whether an encoding label names ISO-8859-1 itself.  WHATWG, and so encoding_rs,
/// maps these labels to windows-1252, which reads bytes 0x80-0x9F as other
/// characters (0x80 as €); an explicit latin-1 means every byte is the code point of
/// the same value.
pub fn is_latin1(label: &str) -> bool {
    matches!(
        label.trim().to_ascii_lowercase().replace('_', "-").as_str(),
        "latin-1" | "latin1" | "iso-8859-1" | "iso8859-1" | "l1" | "iso-ir-100" | "cp819" | "ibm819"
    )
}

/// The encoding a label names (WHATWG labels, also with `_` for `-` as Python spells
/// them); ISO-8859-1 itself is handled apart, see [`is_latin1`].
pub fn encoding_for(label: &str) -> Option<&'static encoding_rs::Encoding> {
    let label = label.trim();
    encoding_rs::Encoding::for_label(label.as_bytes())
        .or_else(|| encoding_rs::Encoding::for_label(label.replace('_', "-").as_bytes()))
}

/// Append ISO-8859-1 bytes to `out` as UTF-8.
pub fn latin1_to_utf8(bytes: &[u8], out: &mut Vec<u8>) {
    for &b in bytes {
        if b < 0x80 {
            out.push(b);
        } else {
            out.extend_from_slice(&[0xC0 | (b >> 6), 0x80 | (b & 0x3F)]);
        }
    }
}

/// Streaming ISO-8859-1 to UTF-8 transcoder.
pub struct Latin1Reader<R> {
    inner: R,
    raw: Vec<u8>,
    out: Vec<u8>,
    pos: usize,
}

impl<R: Read> Latin1Reader<R> {
    pub fn new(inner: R) -> Self {
        Latin1Reader { inner, raw: vec![0; 64 << 10], out: Vec::new(), pos: 0 }
    }
}

impl<R: Read> Read for Latin1Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.out.len() {
            let n = self.inner.read(&mut self.raw)?;
            if n == 0 {
                return Ok(0);
            }
            self.out.clear();
            self.pos = 0;
            latin1_to_utf8(&self.raw[..n], &mut self.out);
        }
        let n = (self.out.len() - self.pos).min(buf.len());
        buf[..n].copy_from_slice(&self.out[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// Strips a UTF-8 byte-order mark from the start of a stream.
pub struct SkipBom<R> {
    inner: R,
    pending: Vec<u8>,
    checked: bool,
}

impl<R: Read> SkipBom<R> {
    pub fn new(inner: R) -> Self {
        SkipBom { inner, pending: Vec::new(), checked: false }
    }
}

impl<R: Read> Read for SkipBom<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.checked {
            self.checked = true;
            let head = read_up_to(&mut self.inner, 3)?;
            if head != b"\xEF\xBB\xBF" {
                self.pending = head;
            }
        }
        if !self.pending.is_empty() {
            let n = self.pending.len().min(buf.len());
            buf[..n].copy_from_slice(&self.pending[..n]);
            self.pending.drain(..n);
            return Ok(n);
        }
        self.inner.read(buf)
    }
}

/// A file written under a temporary name and renamed into place on `commit`.
pub struct AtomicFile {
    tmp: PathBuf,
    target: PathBuf,
    writer: Option<BufWriter<File>>,
}

static SEQ: AtomicU64 = AtomicU64::new(0);

pub fn temp_sibling(target: &Path, tag: &str) -> PathBuf {
    let name = target.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    let nanos =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.subsec_nanos()).unwrap_or(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    target.with_file_name(format!(".{name}.{}.{nanos:x}{seq}.{tag}", std::process::id()))
}

pub fn ensure_writable(target: &Path, overwrite: bool) -> Result<()> {
    if target.exists() && !overwrite {
        return Err(Error::Exists(target.to_path_buf()));
    }
    if let Some(parent) = target.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| Error::io(e, parent))?;
        }
    }
    Ok(())
}

impl AtomicFile {
    pub fn create(target: &Path, overwrite: bool) -> Result<Self> {
        ensure_writable(target, overwrite)?;
        let tmp = temp_sibling(target, "tmp");
        // name the destination in errors: the temporary file is an implementation detail
        let file = File::create(&tmp).map_err(|e| Error::io(e, target))?;
        Ok(AtomicFile { tmp, target: target.to_path_buf(), writer: Some(BufWriter::with_capacity(1 << 20, file)) })
    }

    pub fn writer(&mut self) -> &mut BufWriter<File> {
        self.writer.as_mut().expect("writer used after commit")
    }

    /// Hand the buffered file to a consumer that takes ownership (e.g. a Parquet writer).
    pub fn take_writer(&mut self) -> BufWriter<File> {
        self.writer.take().expect("writer already taken")
    }

    pub fn path(&self) -> &Path {
        &self.tmp
    }

    /// Flush, close and atomically move into place.  Returns the final size in bytes.
    pub fn commit(mut self, writer: Option<BufWriter<File>>) -> Result<u64> {
        let w = writer.or_else(|| self.writer.take());
        if let Some(mut w) = w {
            w.flush().map_err(|e| Error::io(e, &self.tmp))?;
            drop(w);
        }
        let size = std::fs::metadata(&self.tmp).map(|m| m.len()).unwrap_or(0);
        replace(&self.tmp, &self.target)?;
        self.tmp = PathBuf::new();
        Ok(size)
    }
}

impl Drop for AtomicFile {
    fn drop(&mut self) {
        self.writer.take();
        if !self.tmp.as_os_str().is_empty() {
            let _ = std::fs::remove_file(&self.tmp);
        }
    }
}

pub fn replace(from: &Path, to: &Path) -> Result<()> {
    match std::fs::rename(from, to) {
        Ok(()) => Ok(()),
        Err(_) if to.exists() => {
            // Some platforms refuse to rename over an existing file.
            std::fs::remove_file(to).map_err(|e| Error::io(e, to))?;
            std::fs::rename(from, to).map_err(|e| Error::io(e, to))
        }
        Err(e) => Err(Error::io(e, to)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transcodes_windows_1252() {
        let src: &[u8] = b"caf\xe9,\x80\n";
        let mut r = Utf8Reader::new(src, encoding_rs::WINDOWS_1252);
        let mut out = String::new();
        r.read_to_string(&mut out).unwrap();
        assert_eq!(out, "café,€\n");
    }

    #[test]
    fn utf16_with_bom() {
        let mut src = vec![0xFF, 0xFE];
        for ch in "a,b\n".encode_utf16() {
            src.extend_from_slice(&ch.to_le_bytes());
        }
        let mut r = Utf8Reader::new(&src[..], encoding_rs::UTF_16LE);
        let mut out = String::new();
        r.read_to_string(&mut out).unwrap();
        assert_eq!(out, "a,b\n");
    }

    #[test]
    fn skip_bom() {
        let mut out = Vec::new();
        SkipBom::new(&b"\xEF\xBB\xBFx,y"[..]).read_to_end(&mut out).unwrap();
        assert_eq!(out, b"x,y");
        out.clear();
        SkipBom::new(&b"xy"[..]).read_to_end(&mut out).unwrap();
        assert_eq!(out, b"xy");
    }

    #[test]
    fn atomic_file_cleans_up() {
        let dir = std::env::temp_dir().join(format!("vegg-io-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("out.bin");
        {
            let mut f = AtomicFile::create(&target, true).unwrap();
            f.writer().write_all(b"partial").unwrap();
            // dropped without commit
        }
        assert!(!target.exists());
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        let mut f = AtomicFile::create(&target, true).unwrap();
        f.writer().write_all(b"done").unwrap();
        assert_eq!(f.commit(None).unwrap(), 4);
        assert_eq!(std::fs::read(&target).unwrap(), b"done");
        assert!(matches!(AtomicFile::create(&target, false), Err(Error::Exists(_))));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
