//! Byte-exact columnar encoding of one block of delimited text.
//!
//! ```text
//! payload := streams:varint (codec:u8 raw_len:varint stored_len:varint)*  bytes...
//! stream 0 (meta) := version:u8 delimiter:u8 quote:u8 records:varint rows:varint
//!                    cols:varint term_mode:u8 last_term:u8 raw_records:varint
//!                    column_meta*
//! then, in order: [raw records: positions:int_array lengths:int_array bytes]
//!                 [terminators:int_array when term_mode = 255]
//!                 the streams of every column (see `text.rs`)
//! ```
//! A *regular* row has the block's dominant field count and clean quoting, so it is
//! exactly `field (delimiter field)* terminator` with each field optionally wrapped
//! in quotes.  Everything else (the header, ragged or malformed records) is stored
//! verbatim.  Every encoded block is decoded again and compared with the input; on
//! any mismatch the caller falls back to plain compression, so the format can never
//! lose data.

use rayon::prelude::*;

use super::ints::{self, put_varint, Cursor};
use super::text::{self, Cells};
use crate::codec::{self, CodecId, Profile};
use crate::error::{Error, Result};
use crate::tokenize::{Dialect, Term, Tokenizer};

const VERSION: u8 = 1;

pub struct Encoded {
    pub payload: Vec<u8>,
    pub records: u32,
}

/// Codec candidates for a stream under a profile.
pub fn stream_codecs(profile: Profile) -> Vec<(CodecId, i32)> {
    match profile {
        Profile::Fast => vec![(CodecId::Zstd, 1)],
        Profile::Balanced => vec![(CodecId::Zstd, 9)],
        Profile::Max => vec![(CodecId::Zstd, 19), (CodecId::Lzma, 9)],
    }
}

/// One compressed stream of a payload: (codec, decoded length, stored bytes).
pub type Stream = (CodecId, usize, Vec<u8>);

/// Brotli at quality 11 runs at about 1 MB/s, so it is only tried on small streams.
const BROTLI_MAX_STREAM: usize = 1 << 20;

pub fn compress_stream(data: &[u8], profile: Profile) -> Result<(CodecId, Vec<u8>)> {
    if data.len() < 48 {
        return Ok((CodecId::None, data.to_vec()));
    }
    let mut candidates = stream_codecs(profile);
    if profile == Profile::Max && data.len() <= BROTLI_MAX_STREAM {
        candidates.push((CodecId::Brotli, 11));
    }
    codec::compress_best(&candidates, data)
}

/// Quick size estimate used to rank candidate encodings before the expensive codecs run.
pub(crate) fn quick_size(data: &[u8]) -> usize {
    if data.len() < 48 {
        return data.len();
    }
    codec::compress(CodecId::Zstd, data, 3).map(|c| c.len().min(data.len())).unwrap_or(data.len())
}

fn assemble(streams: &[(CodecId, usize, Vec<u8>)]) -> Vec<u8> {
    let total: usize = streams.iter().map(|s| s.2.len() + 12).sum();
    let mut out = Vec::with_capacity(total + 8);
    put_varint(&mut out, streams.len() as u64);
    for (codec, raw_len, data) in streams {
        out.push(*codec as u8);
        put_varint(&mut out, *raw_len as u64);
        put_varint(&mut out, data.len() as u64);
    }
    for (_, _, data) in streams {
        out.extend_from_slice(data);
    }
    out
}

/// Encode a block of complete records.  `first_block` keeps the very first record
/// (usually the header) verbatim.  Returns None when columnar encoding does not apply
/// (no regular rows) or does not reproduce the input.
pub fn encode(data: &[u8], d: Dialect, first_block: bool, profile: Profile) -> Result<Option<Encoded>> {
    let quote = d.quote.unwrap_or(0);
    // Pass 1: dominant field count among regular records.
    let mut tok = Tokenizer::new(d);
    let mut counts: Vec<(usize, usize)> = Vec::new();
    let mut records = 0usize;
    tok.for_each(data, |r| {
        if !(first_block && records == 0) && r.regular {
            let n = r.fields.len();
            match counts.iter_mut().find(|(k, _)| *k == n) {
                Some(e) => e.1 += 1,
                None => counts.push((n, 1)),
            }
        }
        records += 1;
        true
    });
    let Some(&(ncols, regular)) = counts.iter().max_by_key(|&&(k, f)| (f, k)) else {
        return Ok(None);
    };
    if regular * 2 < records {
        return Ok(None);
    }
    // Pass 2: split into raw records and column cells.
    let mut cells: Vec<Vec<&[u8]>> = vec![Vec::with_capacity(regular); ncols];
    let mut quoted: Vec<Vec<bool>> = vec![Vec::with_capacity(regular); ncols];
    let mut terms: Vec<i64> = Vec::with_capacity(regular);
    let mut raw_pos: Vec<i64> = Vec::new();
    let mut raw_len: Vec<i64> = Vec::new();
    let mut raw_bytes: Vec<u8> = Vec::new();
    let mut index = 0usize;
    let mut tok = Tokenizer::new(d);
    tok.for_each(data, |r| {
        let keep_raw = (first_block && index == 0) || !r.regular || r.fields.len() != ncols;
        if keep_raw {
            raw_pos.push(index as i64);
            raw_len.push((r.end - r.start) as i64);
            raw_bytes.extend_from_slice(r.raw());
        } else {
            for (j, f) in r.fields.iter().enumerate() {
                cells[j].push(&data[f.start..f.end]);
                quoted[j].push(f.quoted);
            }
            terms.push(r.term.code() as i64);
        }
        index += 1;
        true
    });
    let rows = terms.len();
    if rows == 0 {
        return Ok(None);
    }
    // Terminators: one code for every row except possibly the last.
    let last_term = *terms.last().unwrap() as u8;
    let body = &terms[..rows - 1];
    let term_mode: u8 = match body.first() {
        None => last_term,
        Some(&t) if body.iter().all(|&x| x == t) => t as u8,
        _ => 255,
    };

    // Columns, in parallel.
    let exhaustive = profile == Profile::Max;
    let columns: Vec<(Vec<u8>, Vec<Stream>)> = (0..ncols)
        .into_par_iter()
        .map(|j| -> Result<_> {
            // Rank the candidate encodings with a fast codec, then spend the profile's
            // codecs only on the winner.
            let candidates = text::candidates(&cells[j], &quoted[j], exhaustive);
            let best = if candidates.len() == 1 {
                candidates.into_iter().next().expect("one candidate")
            } else {
                candidates
                    .into_iter()
                    .min_by_key(|c| c.meta.len() + c.streams.iter().map(|s| quick_size(s) + 6).sum::<usize>())
                    .expect("raw encoding always applies")
            };
            let mut packed = Vec::with_capacity(best.streams.len());
            for s in &best.streams {
                let (c, bytes) = compress_stream(s, profile)?;
                packed.push((c, s.len(), bytes));
            }
            Ok((best.meta, packed))
        })
        .collect::<Result<_>>()?;

    let mut meta = vec![VERSION, d.delimiter, quote];
    put_varint(&mut meta, records as u64);
    put_varint(&mut meta, rows as u64);
    put_varint(&mut meta, ncols as u64);
    meta.push(term_mode);
    meta.push(last_term);
    put_varint(&mut meta, raw_pos.len() as u64);
    let mut streams: Vec<(CodecId, usize, Vec<u8>)> = Vec::new();
    if !raw_pos.is_empty() {
        let mut s = Vec::new();
        ints::encode(&mut s, &raw_pos);
        ints::encode(&mut s, &raw_len);
        s.extend_from_slice(&raw_bytes);
        let (c, bytes) = compress_stream(&s, profile)?;
        streams.push((c, s.len(), bytes));
    }
    if term_mode == 255 {
        let s = ints::encode_to_vec(&terms);
        let (c, bytes) = compress_stream(&s, profile)?;
        streams.push((c, s.len(), bytes));
    }
    for (m, s) in columns {
        meta.extend_from_slice(&m);
        streams.extend(s);
    }
    let (mc, mbytes) = compress_stream(&meta, profile)?;
    let mut all = Vec::with_capacity(streams.len() + 1);
    all.push((mc, meta.len(), mbytes));
    all.extend(streams);
    let payload = assemble(&all);

    // Verify: the encoding must reproduce the block exactly.
    match decode(&payload, data.len()) {
        Ok(back) if back == data => Ok(Some(Encoded { payload, records: records as u32 })),
        _ => Ok(None),
    }
}

pub fn decode(payload: &[u8], raw_len: usize) -> Result<Vec<u8>> {
    let mut c = Cursor::new(payload);
    let n = c.usize()?;
    if n > payload.len() {
        return Err(ints::truncated());
    }
    let mut dir = Vec::with_capacity(n);
    for _ in 0..n {
        let codec = CodecId::from_u8(c.u8()?)?;
        let raw = c.usize()?;
        let stored = c.usize()?;
        dir.push((codec, raw, stored));
    }
    let mut pos = c.pos;
    let mut raw_streams = Vec::with_capacity(n);
    for &(codec, raw, stored) in &dir {
        let bytes = payload.get(pos..pos + stored).ok_or_else(ints::truncated)?;
        pos += stored;
        raw_streams.push((codec, raw, bytes));
    }
    let mut next_stream = raw_streams.into_iter();
    let mut take = move || -> Result<Vec<u8>> {
        let (codec, raw, bytes) = next_stream.next().ok_or_else(ints::truncated)?;
        codec::decompress(codec, bytes, raw)
    };
    let meta_bytes = take()?;
    let mut meta = Cursor::new(&meta_bytes);
    let version = meta.u8()?;
    if version != VERSION {
        return Err(Error::Format(format!("unsupported DCZ block version {version}")));
    }
    let delimiter = meta.u8()?;
    let quote = meta.u8()?;
    let records = meta.usize()?;
    let rows = meta.usize()?;
    let ncols = meta.usize()?;
    let term_mode = meta.u8()?;
    let last_term = meta.u8()?;
    let n_raw = meta.usize()?;
    if rows > records || n_raw > records || ncols > raw_len + 1 {
        return Err(ints::truncated());
    }
    let mut raw_records: Vec<(usize, Vec<u8>)> = Vec::new();
    if n_raw > 0 {
        let s = take()?;
        let mut sc = Cursor::new(&s);
        let positions = ints::decode(&mut sc)?;
        let lengths = ints::decode(&mut sc)?;
        if positions.len() != n_raw || lengths.len() != n_raw {
            return Err(ints::truncated());
        }
        for (p, l) in positions.into_iter().zip(lengths) {
            if p < 0 || l < 0 {
                return Err(ints::truncated());
            }
            raw_records.push((p as usize, sc.bytes(l as usize)?.to_vec()));
        }
    }
    let terms = if term_mode == 255 { Some(ints::decode_all(&take()?)?) } else { None };
    let mut cols: Vec<Cells> = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        cols.push(text::decode_column(&mut meta, &mut take, rows)?);
    }

    let mut out = Vec::with_capacity(raw_len);
    let mut raw_iter = raw_records.into_iter().peekable();
    let mut row = 0usize;
    for rec in 0..records {
        if raw_iter.peek().is_some_and(|(p, _)| *p == rec) {
            out.extend_from_slice(&raw_iter.next().unwrap().1);
            continue;
        }
        if row >= rows {
            return Err(ints::truncated());
        }
        for (j, col) in cols.iter().enumerate() {
            if j > 0 {
                out.push(delimiter);
            }
            let q = col.quoted[row];
            if q {
                out.push(quote);
            }
            out.extend_from_slice(col.get(row));
            if q {
                out.push(quote);
            }
        }
        let code = if row + 1 == rows {
            last_term
        } else {
            match &terms {
                Some(t) => *t.get(row).ok_or_else(ints::truncated)? as u8,
                None => term_mode,
            }
        };
        out.extend_from_slice(Term::from_code(code).ok_or_else(ints::truncated)?.bytes());
        row += 1;
    }
    if row != rows || out.len() != raw_len {
        return Err(Error::Integrity(format!("DCZ block decoded to {} bytes, expected {}", out.len(), raw_len)));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(data: &[u8], expect_columnar: bool) {
        for profile in [Profile::Balanced, Profile::Max] {
            let enc = encode(data, Dialect::default(), true, profile).unwrap();
            assert_eq!(enc.is_some(), expect_columnar, "{profile:?}");
            if let Some(e) = enc {
                assert_eq!(decode(&e.payload, data.len()).unwrap(), data);
            }
        }
    }

    #[test]
    fn roundtrips_tricky_csv() {
        let mut csv = String::from("id,price,when,name,note\r\n");
        for i in 0..2000 {
            let note = match i % 9 {
                0 => "\"with, comma\"".to_string(),
                1 => "\"multi\nline\"".to_string(),
                2 => String::new(),
                3 => "\"\"".to_string(),
                4 => "\"say \"\"hi\"\"\"".to_string(),
                _ => format!("n{}", i % 13),
            };
            let term = if i % 101 == 0 { "\n" } else { "\r\n" };
            csv.push_str(&format!(
                "{},{}.{:02},2024-03-{:02}T12:{:02}:00,user_{:05},{}{}",
                i * 3,
                i % 500,
                i % 100,
                i % 28 + 1,
                i % 60,
                i * 7 % 10000,
                note,
                term
            ));
        }
        csv.push_str("bad,row\r\n\"unterminated,1,2,3,4");
        check(csv.as_bytes(), true);
    }

    #[test]
    fn compresses_numeric_csv_well() {
        let mut csv = String::from("ts,value,sensor\n");
        for i in 0..50_000 {
            csv.push_str(&format!("{},{}.{},S{}\n", 1_700_000_000 + i * 10, 20 + i % 7, i % 10, i % 4));
        }
        let enc = encode(csv.as_bytes(), Dialect::default(), true, Profile::Balanced).unwrap().unwrap();
        let zstd = codec::compress(CodecId::Zstd, csv.as_bytes(), 19).unwrap();
        assert!(enc.payload.len() * 2 < zstd.len(), "columnar {} vs zstd-19 {}", enc.payload.len(), zstd.len());
    }

    #[test]
    fn falls_back_when_irregular() {
        check(b"just one line without delimiters", false);
        check(b"", false);
    }

    #[test]
    fn no_trailing_newline_and_single_column() {
        check(b"h\n1\n2\n3", true);
        check(b"a,b\n1,2\n3,4\n", true);
    }
}
