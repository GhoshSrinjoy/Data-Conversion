//! Record-aligned chunking.
//!
//! A chunk always ends at a record terminator that the tokenizer sees outside
//! quotes, so chunks can be parsed independently.  The scan follows the tokenizer's
//! rules exactly: a quote opens a quoted field only at the start of a field (anywhere
//! else it is a literal character, like the `"` in `55" screen`), and inside a quoted
//! field a doubled or escaped quote is literal.  Between quoted fields the scan runs
//! at memchr speed.  Every chunk starts at a record boundary, so scanning from the
//! chunk start is enough.
//!
//! A quote that is never closed would turn the rest of the input into one field.  It
//! is treated as a stray instead: when a quoted field is still open at the end of the
//! input, or [`MAX_QUOTED`] bytes after its record started, the chunk ends with the
//! line that holds the opening quote.  The tokenizer reports that record as
//! unterminated (a malformed row) and parsing resumes on the next line.

use std::io::Read;
use std::ops::Range;

use rayon::prelude::*;

use crate::error::{Error, Result};
use crate::tokenize::{Dialect, Tokenizer};

/// A quoted field still open this many bytes after its record started is taken to
/// be a stray quote.
pub const MAX_QUOTED: usize = 128 << 20;

#[inline]
fn at_field_start(buf: &[u8], j: usize, d: &Dialect) -> bool {
    j == 0 || {
        let b = buf[j - 1];
        b == d.delimiter || b == b'\n' || b == b'\r'
    }
}

/// Visit the stretches of `buf` that lie outside quoted fields, in order, scanning
/// from a record boundary.  Returns the offset of the opening quote when `buf` ends
/// inside a quoted field.
fn outside<F: FnMut(usize, usize)>(buf: &[u8], d: &Dialect, mut visit: F) -> Option<usize> {
    let n = buf.len();
    let Some(q) = d.quote else {
        visit(0, n);
        return None;
    };
    let esc = d.escape.filter(|&e| e != q);
    let (mut pos, mut search) = (0usize, 0usize);
    loop {
        // The next quote that opens a field; others are literal characters.
        let open = loop {
            match memchr::memchr(q, &buf[search..]) {
                None => break None,
                Some(k) if at_field_start(buf, search + k, d) => break Some(search + k),
                Some(k) => search += k + 1,
            }
        };
        let Some(j) = open else {
            visit(pos, n);
            return None;
        };
        visit(pos, j);
        // The quote that closes it.
        let mut i = j + 1;
        loop {
            let rest = &buf[i.min(n)..];
            let hit = match esc {
                Some(e) => memchr::memchr2(q, e, rest),
                None => memchr::memchr(q, rest),
            };
            let Some(k) = hit else { return Some(j) };
            let p = i + k;
            // an escaped character, or a doubled quote, is part of the field
            if Some(buf[p]) == esc || (d.double_quote && p + 1 < n && buf[p + 1] == q) {
                i = p + 2;
            } else {
                pos = p + 1;
                search = p + 1;
                break;
            }
        }
    }
}

/// Whether a record may end after the terminator byte at `t`.  With CR line endings
/// a CR followed by LF is half of a CRLF pair (and at the end of the buffer the next
/// byte is unknown).
#[inline]
fn is_cut(buf: &[u8], t: usize, term: u8) -> bool {
    term != b'\r' || buf.get(t + 1).is_some_and(|&b| b != b'\n')
}

fn last_term(buf: &[u8], start: usize, end: usize, term: u8) -> Option<usize> {
    let mut end = end;
    while let Some(k) = memchr::memrchr(term, &buf[start..end]) {
        let t = start + k;
        if is_cut(buf, t, term) {
            return Some(t + 1);
        }
        end = t;
    }
    None
}

/// Stretches outside quotes remembered by the fast scan; a record that holds more
/// quoted fields than this without a line break falls back to the full scan.
const RECENT: usize = 256;

/// Offset just past the last record terminator outside quotes, and the opening quote
/// of a quoted field still open at the end of `buf`.
pub fn scan_end(buf: &[u8], d: &Dialect, term: u8) -> (Option<usize>, Option<usize>) {
    let Some(q) = d.quote else { return (last_term(buf, 0, buf.len(), term), None) };
    if term == b'\n' && d.double_quote && d.escape.is_none_or(|e| e == q) {
        if let Some(found) = scan_regular(buf, q, d.delimiter) {
            return found;
        }
    }
    match d.escape.filter(|&e| e != q) {
        Some(e) => scan_quotes(buf, d, term, memchr::memchr2_iter(q, e, buf)),
        None => scan_quotes(buf, d, term, memchr::memchr_iter(q, buf)),
    }
    .unwrap_or_else(|| scan_end_full(buf, d, term))
}

/// Quotes, structural bytes (delimiter or line break) and line feeds of 64 bytes as
/// bit masks.
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline(always)]
fn masks(block: &[u8; 64], q: u8, delimiter: u8) -> (u64, u64, u64) {
    let (mut quotes, mut structural, mut lf) = (0u64, 0u64, 0u64);
    for (i, &c) in block.iter().enumerate() {
        quotes |= u64::from(c == q) << i;
        structural |= u64::from(c == delimiter || c == b'\n' || c == b'\r') << i;
        lf |= u64::from(c == b'\n') << i;
    }
    (quotes, structural, lf)
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn masks(block: &[u8; 64], q: u8, delimiter: u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    // SAFETY: SSE2 is part of the x86-64 baseline, and every load reads 16 bytes
    // inside the 64-byte block.
    unsafe {
        let (vq, vd) = (_mm_set1_epi8(q as i8), _mm_set1_epi8(delimiter as i8));
        let (vlf, vcr) = (_mm_set1_epi8(b'\n' as i8), _mm_set1_epi8(b'\r' as i8));
        let (mut quotes, mut structural, mut lf) = (0u64, 0u64, 0u64);
        for k in 0..4 {
            let v = _mm_loadu_si128(block.as_ptr().add(k * 16) as *const __m128i);
            let is_lf = _mm_cmpeq_epi8(v, vlf);
            let is_s = _mm_or_si128(_mm_or_si128(_mm_cmpeq_epi8(v, vd), is_lf), _mm_cmpeq_epi8(v, vcr));
            quotes |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, vq)) as u16 as u64) << (k * 16);
            structural |= (_mm_movemask_epi8(is_s) as u16 as u64) << (k * 16);
            lf |= (_mm_movemask_epi8(is_lf) as u16 as u64) << (k * 16);
        }
        (quotes, structural, lf)
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn masks(block: &[u8; 64], q: u8, delimiter: u8) -> (u64, u64, u64) {
    use std::arch::aarch64::*;
    // SAFETY: NEON is part of the AArch64 baseline, and every load reads 16 bytes
    // inside the 64-byte block.
    unsafe {
        const WEIGHTS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let weights = vld1q_u8(WEIGHTS.as_ptr());
        let movemask = |v: uint8x16_t| -> u64 {
            let m = vandq_u8(v, weights);
            (vaddv_u8(vget_low_u8(m)) as u64) | ((vaddv_u8(vget_high_u8(m)) as u64) << 8)
        };
        let (vq, vd, vlf, vcr) = (vdupq_n_u8(q), vdupq_n_u8(delimiter), vdupq_n_u8(b'\n'), vdupq_n_u8(b'\r'));
        let (mut quotes, mut structural, mut lf) = (0u64, 0u64, 0u64);
        for k in 0..4 {
            let v = vld1q_u8(block.as_ptr().add(k * 16));
            let is_lf = vceqq_u8(v, vlf);
            let is_s = vorrq_u8(vorrq_u8(vceqq_u8(v, vd), is_lf), vceqq_u8(v, vcr));
            quotes |= movemask(vceqq_u8(v, vq)) << (k * 16);
            structural |= movemask(is_s) << (k * 16);
            lf |= movemask(is_lf) << (k * 16);
        }
        (quotes, structural, lf)
    }
}

/// RFC 4180 fast path of [`scan_end`] for line-feed records: the quote state is the
/// parity of the quotes seen, computed 64 bytes at a time with a prefix XOR.  That
/// equals the tokenizer's state as long as every quote is regular: one that opens a
/// field sits at the start of a field (or is the second of a doubled pair), and one
/// that closes a field is followed by a delimiter, a line break, another quote or the
/// end.  None as soon as a quote is not, and the exact scan decides.
fn scan_regular(buf: &[u8], q: u8, delimiter: u8) -> Option<(Option<usize>, Option<usize>)> {
    let n = buf.len();
    let (mut inside, mut prev_sq, mut prev_s) = (0u64, 1u64, 1u64);
    let (mut last, mut open) = (None, None);
    let mut start = 0;
    while start < n {
        let width = (n - start).min(64);
        let (quotes, structural, lf) = match buf[start..].first_chunk::<64>() {
            Some(block) => masks(block, q, delimiter),
            None => {
                // the short last block, padded with bytes that are none of the three
                let mut block = [0u8; 64];
                block[..width].copy_from_slice(&buf[start..]);
                let (mut quotes, mut structural, mut lf) = masks(&block, q, delimiter);
                if q == 0 || delimiter == 0 {
                    let valid = (1u64 << width) - 1;
                    (quotes, structural, lf) = (quotes & valid, structural & valid, lf & valid);
                }
                (quotes, structural, lf)
            }
        };
        let valid = if width == 64 { !0 } else { (1u64 << width) - 1 };
        let next_sq = match buf.get(start + width) {
            Some(&c) => u64::from(c == q || c == delimiter || c == b'\n' || c == b'\r'),
            None => 1,
        };
        let mut parity = quotes;
        for shift in [1, 2, 4, 8, 16, 32] {
            parity ^= parity << shift;
        }
        let after = parity ^ inside; // inside a quoted field after each byte
        let before = (after << 1) | (inside & 1);
        let sq = structural | quotes;
        let before_sq = (sq << 1) | prev_sq;
        let after_sq = (sq >> 1) | (next_sq << (width - 1));
        if quotes & ((!before & !before_sq) | (before & !after_sq)) & valid != 0 {
            return None;
        }
        let ends = lf & !after & valid;
        if ends != 0 {
            last = Some(start + 64 - ends.leading_zeros() as usize);
        }
        let opens = quotes & !before & ((structural << 1) | prev_s) & valid;
        if opens != 0 {
            open = Some(start + 63 - opens.leading_zeros() as usize);
        }
        inside = if (after >> (width - 1)) & 1 == 1 { !0 } else { 0 };
        prev_sq = (sq >> (width - 1)) & 1;
        prev_s = (structural >> (width - 1)) & 1;
        start += width;
    }
    Some((last, if inside != 0 { open } else { None }))
}

/// One pass over the quote (and escape) positions, the same rules as [`outside`]:
/// the state only changes at those bytes, so the stretches between them need no
/// scanning.  The last few outside stretches are kept to find the last record end.
/// None when that is not enough (see [`RECENT`]).
fn scan_quotes(
    buf: &[u8],
    d: &Dialect,
    term: u8,
    positions: impl Iterator<Item = usize>,
) -> Option<(Option<usize>, Option<usize>)> {
    let n = buf.len();
    let q = d.quote.expect("quoted dialect");
    let esc = d.escape.filter(|&e| e != q);
    let mut recent = [(0usize, 0usize); RECENT];
    let mut stretches = 0usize;
    let (mut inside, mut open, mut region, mut skip_to) = (false, 0usize, 0usize, 0usize);
    for j in positions {
        if j < skip_to {
            continue;
        }
        let b = buf[j];
        if inside {
            // an escaped character, or a doubled quote, is part of the field
            if Some(b) == esc || (d.double_quote && j + 1 < n && buf[j + 1] == q) {
                skip_to = j + 2;
            } else {
                inside = false;
                region = j + 1;
            }
        } else if b == q && at_field_start(buf, j, d) {
            if j > region {
                recent[stretches % RECENT] = (region, j);
                stretches += 1;
            }
            inside = true;
            open = j;
        }
    }
    if !inside && n > region {
        recent[stretches % RECENT] = (region, n);
        stretches += 1;
    }
    for k in 0..stretches.min(RECENT) {
        let (s, e) = recent[(stretches - 1 - k) % RECENT];
        if let Some(t) = last_term(buf, s, e, term) {
            return Some((Some(t), inside.then_some(open)));
        }
    }
    (stretches <= RECENT).then_some((None, inside.then_some(open)))
}

/// [`scan_end`] by visiting every stretch outside quotes.
fn scan_end_full(buf: &[u8], d: &Dialect, term: u8) -> (Option<usize>, Option<usize>) {
    let mut last = None;
    let open = outside(buf, d, |s, e| {
        if let Some(t) = last_term(buf, s, e, term) {
            last = Some(t);
        }
    });
    (last, open)
}

/// Offset just past the last record terminator outside quotes.
pub fn last_record_end(buf: &[u8], d: &Dialect, term: u8) -> Option<usize> {
    scan_end(buf, d, term).0
}

/// Split `buf` (whole records) into about `n` ranges at record boundaries.
pub fn split_records(buf: &[u8], d: &Dialect, term: u8, n: usize) -> Vec<Range<usize>> {
    let n = n.max(1);
    let mut cuts = vec![0usize];
    let mut k = 1;
    outside(buf, d, |s, e| {
        let mut from = s;
        while k < n {
            let lo = from.max(buf.len() * k / n);
            if lo >= e {
                break;
            }
            let Some(t) = memchr::memchr_iter(term, &buf[lo..e]).map(|t| lo + t).find(|&t| is_cut(buf, t, term)) else {
                break;
            };
            cuts.push(t + 1);
            from = t + 1;
            while k < n && buf.len() * k / n <= t {
                k += 1;
            }
        }
    });
    cuts.push(buf.len());
    cuts.dedup();
    cuts.windows(2).filter(|w| w[1] > w[0]).map(|w| w[0]..w[1]).collect()
}

pub struct Chunk {
    pub data: Vec<u8>,
    /// Offset of `data[0]` within the (decoded) stream.
    pub offset: u64,
    pub index: usize,
}

/// Splits a byte stream into record-aligned chunks of roughly `chunk_size` bytes.
pub struct Chunker<R> {
    reader: R,
    chunk_size: usize,
    dialect: Dialect,
    term: u8,
    carry: Vec<u8>,
    offset: u64,
    index: usize,
    eof: bool,
    /// A record without a line break longer than this is an error (it would not fit
    /// in memory as one chunk), or with `cut_anywhere` is cut where it stands.
    max_record: usize,
    cut_anywhere: bool,
}

impl<R: Read> Chunker<R> {
    /// A chunker for records quoted with `quote` (None: plain lines) and ended by
    /// `term`.  CSV callers pass their full dialect with [`Chunker::with_dialect`].
    pub fn new(reader: R, chunk_size: usize, quote: Option<u8>, term: u8) -> Self {
        Chunker {
            reader,
            chunk_size: chunk_size.max(1),
            dialect: Dialect { quote, ..Dialect::default() },
            term,
            carry: Vec::new(),
            offset: 0,
            index: 0,
            eof: false,
            max_record: (chunk_size.max(1) * 8).max(MAX_QUOTED),
            cut_anywhere: false,
        }
    }

    /// The CSV dialect (delimiter, quote, escape) that decides where fields start.
    pub fn with_dialect(mut self, d: Dialect) -> Self {
        self.dialect = d;
        self
    }

    /// For consumers that keep bytes rather than parse records (DCZ): cut chunks at
    /// `n` bytes wherever no record boundary is found.
    pub fn cut_at(mut self, n: usize) -> Self {
        self.max_record = n.max(self.chunk_size);
        self.cut_anywhere = true;
        self
    }

    /// Prepend bytes that were already read from the stream (e.g. by the sniffer).
    pub fn with_prefix(mut self, prefix: Vec<u8>) -> Self {
        self.carry = prefix;
        self
    }

    fn fill(&mut self, target: usize) -> Result<()> {
        while !self.eof && self.carry.len() < target {
            let want = target - self.carry.len();
            let start = self.carry.len();
            self.carry.resize(start + want, 0);
            let mut filled = start;
            while filled < self.carry.len() {
                match self.reader.read(&mut self.carry[filled..]) {
                    Ok(0) => {
                        self.eof = true;
                        break;
                    }
                    Ok(n) => filled += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => {
                        self.carry.truncate(filled);
                        return Err(match e.kind() {
                            std::io::ErrorKind::InvalidData => Error::Data(format!("input decoding failed: {e}")),
                            _ => Error::from(e),
                        });
                    }
                }
            }
            self.carry.truncate(filled);
        }
        Ok(())
    }

    /// Where the next chunk ends, or None to read more.
    fn cut(&self, target: usize) -> Result<Option<usize>> {
        let len = self.carry.len();
        let all_read = self.eof && len < target;
        if self.cut_anywhere && len >= self.max_record {
            return Ok(Some(last_record_end(&self.carry, &self.dialect, self.term).unwrap_or(len)));
        }
        let (last, open) = scan_end(&self.carry, &self.dialect, self.term);
        if let Some(j) = open {
            let record = last.unwrap_or(0);
            if self.eof || len - record >= MAX_QUOTED {
                // A stray quote: the chunk ends with the line that holds it.
                if last.is_some() && !all_read && len - record < MAX_QUOTED {
                    return Ok(last);
                }
                let line_end = memchr::memchr_iter(self.term, &self.carry[j..])
                    .map(|k| j + k)
                    .find(|&t| is_cut(&self.carry, t, self.term))
                    .map_or(len, |t| t + 1);
                return Ok(Some(line_end));
            }
            return Ok(last);
        }
        if all_read {
            return Ok(Some(len));
        }
        if last.is_some() {
            return Ok(last);
        }
        if self.eof {
            return Ok(Some(len));
        }
        if len >= self.max_record {
            return Err(Error::Data(format!(
                "a record runs for more than {} MiB without a line break (raise the chunk size if that is intended)",
                self.max_record >> 20
            )));
        }
        Ok(None)
    }

    pub fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        let mut target = self.chunk_size;
        loop {
            self.fill(target)?;
            if self.carry.is_empty() {
                return Ok(None);
            }
            if let Some(cut) = self.cut(target)? {
                let rest = self.carry.split_off(cut);
                let data = std::mem::replace(&mut self.carry, rest);
                let chunk = Chunk { data, offset: self.offset, index: self.index };
                self.offset += cut as u64;
                self.index += 1;
                return Ok(Some(chunk));
            }
            // A single record longer than the target: read further, but no further
            // than the limits that decide what happens to it.
            let limit = if self.cut_anywhere { self.max_record } else { self.max_record.max(MAX_QUOTED + 1) };
            target = self.carry.len().saturating_mul(2).max(self.chunk_size).min(limit.max(self.carry.len() + 1));
        }
    }

    pub fn into_parts(self) -> (R, Vec<u8>) {
        (self.reader, self.carry)
    }
}

/// Count records in a stream exactly as the tokenizer splits them (a final record
/// without a terminator counts too).  Blank lines are skipped unless `count_blank`
/// (a single-column file, where a blank line is a missing value).
pub fn count_records<R: Read>(reader: R, d: Dialect, term: u8, count_blank: bool) -> Result<u64> {
    let mut chunker = Chunker::new(reader, 8 << 20, d.quote, term).with_dialect(d);
    let wave = rayon::current_num_threads().max(1);
    let mut records = 0u64;
    loop {
        let mut chunks = Vec::with_capacity(wave);
        while chunks.len() < wave {
            match chunker.next_chunk()? {
                Some(c) => chunks.push(c),
                None => break,
            }
        }
        if chunks.is_empty() {
            return Ok(records);
        }
        records += chunks
            .par_iter()
            .map(|c| {
                let mut n = 0u64;
                Tokenizer::new(d).for_each(&c.data, |r| {
                    if count_blank || !r.is_blank() {
                        n += 1;
                    }
                    true
                });
                n
            })
            .sum::<u64>();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn csv() -> Dialect {
        Dialect::default()
    }

    #[test]
    fn boundaries_respect_quotes() {
        let d = csv();
        assert_eq!(last_record_end(b"a,b\n1,\"x\ny\"\n2,z", &d, b'\n'), Some(12));
        assert_eq!(scan_end(b"a,b\n1,\"x\ny", &d, b'\n'), (Some(4), Some(6)));
        assert_eq!(last_record_end(b"no newline", &d, b'\n'), None);
        // a quote inside an unquoted field is a literal character
        assert_eq!(last_record_end(b"1,55\" screen,9\n2,x\n", &d, b'\n'), Some(19));
        // text after a closing quote is literal, quotes included
        assert_eq!(scan_end(b"\"a\"b\"c,\"d\ne\"\n", &d, b'\n'), (Some(13), None));
        // a field opens only after the dialect's delimiter
        let semi = Dialect { delimiter: b';', ..csv() };
        assert_eq!(scan_end(b"1;\"x\ny", &semi, b'\n'), (None, Some(2)));
        assert_eq!(scan_end(b"1,\"x\ny", &semi, b'\n'), (Some(5), None));
    }

    #[test]
    fn fast_scan_matches_full_scan() {
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let dialects = [
            csv(),
            Dialect { delimiter: b';', ..csv() },
            Dialect { escape: Some(b'\\'), double_quote: false, ..csv() },
            Dialect { double_quote: false, ..csv() },
        ];
        let alphabet = b"ab,;\"\"\n\r\\";
        for round in 0..20_000 {
            let len = (next() % 40) as usize + if round % 100 == 0 { 2000 } else { 0 };
            let buf: Vec<u8> = (0..len).map(|_| alphabet[(next() % alphabet.len() as u64) as usize]).collect();
            for d in &dialects {
                for term in [b'\n', b'\r'] {
                    assert_eq!(
                        scan_end(&buf, d, term),
                        scan_end_full(&buf, d, term),
                        "{:?} {d:?}",
                        String::from_utf8_lossy(&buf)
                    );
                }
            }
        }
    }

    #[test]
    fn splits_at_record_boundaries() {
        let mut data = Vec::new();
        for i in 0..2000 {
            data.extend_from_slice(format!("{i},\"multi\nline {i}\",x\"y\n").as_bytes());
        }
        let ranges = split_records(&data, &csv(), b'\n', 8);
        assert!(ranges.len() > 4);
        assert_eq!(ranges.first().unwrap().start, 0);
        assert_eq!(ranges.last().unwrap().end, data.len());
        for r in &ranges {
            let mut records = 0;
            Tokenizer::new(csv()).for_each(&data[r.clone()], |rec| {
                assert!(rec.regular || rec.fields.len() == 3);
                records += 1;
                true
            });
            assert!(records > 0);
        }
        let total: usize = ranges
            .iter()
            .map(|r| {
                let mut n = 0;
                Tokenizer::new(csv()).for_each(&data[r.clone()], |_| {
                    n += 1;
                    true
                });
                n
            })
            .sum();
        assert_eq!(total, 2000);
    }

    #[test]
    fn chunker_reassembles_exactly() {
        let mut data = Vec::new();
        for i in 0..5000 {
            if i % 7 == 0 {
                data.extend_from_slice(format!("{i},\"multi\nline \"\"{i}\"\"\"\n").as_bytes());
            } else if i % 11 == 0 {
                data.extend_from_slice(format!("{i},12\" pipe\n").as_bytes());
            } else {
                data.extend_from_slice(format!("{i},plain\n").as_bytes());
            }
        }
        data.extend_from_slice(b"last,row");
        for size in [1, 7, 64, 1000, 1 << 20] {
            let mut chunker = Chunker::new(&data[..], size, Some(b'"'), b'\n').with_dialect(csv());
            let mut out = Vec::new();
            while let Some(c) = chunker.next_chunk().unwrap() {
                assert_eq!(c.offset as usize, out.len());
                // every chunk ends at a real record boundary
                let mut last_term = None;
                Tokenizer::new(csv()).for_each(&c.data, |r| {
                    last_term = Some(r.term);
                    true
                });
                if out.len() + c.data.len() < data.len() {
                    assert_eq!(*c.data.last().unwrap(), b'\n');
                    assert_ne!(last_term, Some(crate::tokenize::Term::Eof), "size {size}");
                }
                out.extend_from_slice(&c.data);
            }
            assert_eq!(out, data);
        }
    }

    #[test]
    fn stray_quotes_end_at_their_line() {
        // the quote before note10 is never closed
        let mut data = String::new();
        for i in 0..3000 {
            if i == 10 {
                data.push_str("10,name10,\"note10\n");
            } else {
                data.push_str(&format!("{i},name{i},note{i}\n"));
            }
        }
        for size in [64, 4096, 1 << 20] {
            let mut chunker = Chunker::new(data.as_bytes(), size, Some(b'"'), b'\n').with_dialect(csv());
            let (mut records, mut unterminated) = (0, Vec::new());
            while let Some(c) = chunker.next_chunk().unwrap() {
                Tokenizer::new(csv()).for_each(&c.data, |r| {
                    if r.unterminated {
                        unterminated.push(String::from_utf8_lossy(r.raw()).into_owned());
                    }
                    records += 1;
                    true
                });
            }
            assert_eq!(records, 3000, "size {size}");
            assert_eq!(unterminated, vec!["10,name10,\"note10\n".to_string()], "size {size}");
        }
    }

    #[test]
    fn escapes_and_long_records() {
        let esc = Dialect { escape: Some(b'\\'), double_quote: false, ..csv() };
        let data = b"1,\"a \\\" b\nc\",7\n2,x\n";
        assert_eq!(last_record_end(&data[..16], &esc, b'\n'), Some(15));
        assert_eq!(last_record_end(data, &esc, b'\n'), Some(data.len()));
        // a CR followed by LF is not a record end with CR terminators
        assert_eq!(last_record_end(b"a\rb\r\nc", &csv(), b'\r'), Some(2));
        // DCZ cuts input without terminators at its limit; record parsing refuses it
        let blob = vec![b'x'; 40 << 20];
        let mut chunker = Chunker::new(&blob[..], 4 << 20, None, b'\n').cut_at(8 << 20);
        assert_eq!(chunker.next_chunk().unwrap().unwrap().data.len(), 8 << 20);
        let mut chunker = Chunker::new(&blob[..], 1 << 20, None, b'\n');
        chunker.max_record = 16 << 20;
        assert!(chunker.next_chunk().is_err());
    }

    #[test]
    fn counts_records() {
        let d = csv();
        let data = b"h1,h2\n1,\"a\nb\"\n2,c\n3,d";
        assert_eq!(count_records(&data[..], d, b'\n', false).unwrap(), 4);
        assert_eq!(count_records(&b"a\nb\n"[..], d, b'\n', false).unwrap(), 2);
        assert_eq!(count_records(&b""[..], d, b'\n', false).unwrap(), 0);
        // blank lines are not records, except in single-column files
        assert_eq!(count_records(&b"a,b\r\n1,2\r\n\r\n3,4\n\n"[..], d, b'\n', false).unwrap(), 3);
        assert_eq!(count_records(&b"a\nx\n\nz\n"[..], d, b'\n', true).unwrap(), 4);
        // an escaped quote does not open or close a field
        let esc = Dialect { escape: Some(b'\\'), double_quote: false, ..d };
        assert_eq!(count_records(&b"id,t\n1,\"a \\\" b\nc\",7\n2,x\n"[..], esc, b'\n', false).unwrap(), 3);
        // a literal quote inside a field, lone CRs, and a quote that is never closed
        assert_eq!(count_records(&b"a,b\n1,na\"me\n2,x\n3,y\n"[..], d, b'\n', false).unwrap(), 4);
        assert_eq!(count_records(&b"a,b\r1,2\r3,4\r5,6"[..], d, b'\r', false).unwrap(), 4);
        assert_eq!(count_records(&b"a,b\n1,\"x\n2,y\n"[..], d, b'\n', false).unwrap(), 3);
    }
}
