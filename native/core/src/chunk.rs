//! Record-aligned chunking.
//!
//! A chunk always ends at a record terminator that lies outside quotes.  For RFC
//! 4180 CSV quote state is tracked by parity: an escaped quote is written `""`, so
//! a terminator is inside a quoted field exactly when an odd number of quote
//! characters precede it in the record.  Dialects with an escape character (`\"`)
//! break that rule and use an exact byte-by-byte scan instead.  Every chunk starts
//! at a record boundary, so counting from the chunk start is enough.

use std::io::Read;

use crate::error::{Error, Result};

#[inline]
fn count_byte(haystack: &[u8], needle: u8) -> usize {
    // Written so LLVM vectorises it into compare + popcount.
    haystack.iter().filter(|&&b| b == needle).count()
}

/// Offset just past the last record terminator in `buf` that is outside quotes.
pub fn last_boundary(buf: &[u8], quote: Option<u8>, term: u8) -> Option<usize> {
    let q = match quote {
        None => return memchr::memrchr(term, buf).map(|i| i + 1),
        Some(q) => q,
    };
    let total = count_byte(buf, q);
    if total == 0 {
        return memchr::memrchr(term, buf).map(|i| i + 1);
    }
    let mut after = 0usize;
    let mut end = buf.len();
    while let Some(nl) = memchr::memrchr(term, &buf[..end]) {
        after += count_byte(&buf[nl + 1..end], q);
        if (total - after).is_multiple_of(2) {
            return Some(nl + 1);
        }
        end = nl;
    }
    None
}

/// Exact scan for dialects with an escape character inside quotes.
fn last_boundary_escaped(buf: &[u8], q: u8, esc: u8, term: u8) -> Option<usize> {
    let mut inside = false;
    let mut last = None;
    let mut i = 0;
    while i < buf.len() {
        let b = buf[i];
        if inside {
            if b == esc && esc != q {
                i += 2;
                continue;
            }
            if b == q {
                inside = false;
            }
        } else if b == q {
            inside = true;
        } else if b == term {
            last = Some(i + 1);
        }
        i += 1;
    }
    last
}

/// Offset just past the last record terminator outside quotes, for any dialect.
pub fn last_record_end(buf: &[u8], quote: Option<u8>, escape: Option<u8>, term: u8) -> Option<usize> {
    match (quote, escape) {
        (Some(q), Some(e)) if e != q => last_boundary_escaped(buf, q, e, term),
        _ => last_boundary(buf, quote, term),
    }
}

/// Offset just past the first record terminator outside quotes, starting from a
/// position known to be a record boundary.
pub fn first_boundary(buf: &[u8], quote: Option<u8>, term: u8) -> Option<usize> {
    let mut quotes = 0usize;
    let mut start = 0usize;
    while let Some(i) = memchr::memchr(term, &buf[start..]) {
        let nl = start + i;
        if let Some(q) = quote {
            quotes += count_byte(&buf[start..nl], q);
        }
        if quotes.is_multiple_of(2) {
            return Some(nl + 1);
        }
        start = nl + 1;
    }
    None
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
    quote: Option<u8>,
    escape: Option<u8>,
    term: u8,
    carry: Vec<u8>,
    offset: u64,
    index: usize,
    eof: bool,
    /// Records larger than this are not searched for a quote-balanced boundary; the
    /// chunk is cut at the last terminator, or anywhere if there is none (bounds
    /// memory for a stray unbalanced quote or input without line breaks).
    max_record: usize,
}

impl<R: Read> Chunker<R> {
    pub fn new(reader: R, chunk_size: usize, quote: Option<u8>, term: u8) -> Self {
        Chunker {
            reader,
            chunk_size: chunk_size.max(1),
            quote,
            escape: None,
            term,
            carry: Vec::new(),
            offset: 0,
            index: 0,
            eof: false,
            max_record: (chunk_size.max(1) * 8).max(64 << 20),
        }
    }

    /// Escape character inside quoted fields (e.g. `\\`), if the dialect has one.
    pub fn with_escape(mut self, escape: Option<u8>) -> Self {
        self.escape = escape;
        self
    }

    /// Cut chunks without a record boundary once they reach `n` bytes.
    pub fn with_max_record(mut self, n: usize) -> Self {
        self.max_record = n.max(self.chunk_size);
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

    pub fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        let mut target = self.chunk_size;
        loop {
            self.fill(target)?;
            if self.carry.is_empty() {
                return Ok(None);
            }
            let cut = if self.eof && self.carry.len() < target {
                Some(self.carry.len())
            } else {
                let mut cut = last_record_end(&self.carry, self.quote, self.escape, self.term);
                if cut.is_none() && self.carry.len() >= self.max_record {
                    cut = Some(memchr::memrchr(self.term, &self.carry).map_or(self.carry.len(), |i| i + 1));
                }
                cut
            };
            match cut {
                Some(cut) if cut > 0 => {
                    let rest = self.carry.split_off(cut);
                    let data = std::mem::replace(&mut self.carry, rest);
                    let chunk = Chunk { data, offset: self.offset, index: self.index };
                    self.offset += cut as u64;
                    self.index += 1;
                    return Ok(Some(chunk));
                }
                _ if self.eof => {
                    let data = std::mem::take(&mut self.carry);
                    let chunk = Chunk { offset: self.offset, index: self.index, data };
                    self.offset += chunk.data.len() as u64;
                    self.index += 1;
                    return Ok(Some(chunk));
                }
                _ => {
                    // A single record longer than the target: read further.
                    target = self.carry.len().saturating_mul(2).max(self.chunk_size);
                }
            }
        }
    }

    pub fn into_parts(self) -> (R, Vec<u8>) {
        (self.reader, self.carry)
    }
}

/// Count records (terminators outside quotes) in a stream; a final record without a
/// terminator counts too.  Blank lines are skipped unless `count_blank` (a
/// single-column file, where a blank line is a missing value).
pub fn count_records<R: Read>(
    reader: R,
    quote: Option<u8>,
    escape: Option<u8>,
    term: u8,
    count_blank: bool,
) -> Result<u64> {
    let mut chunker = Chunker::new(reader, 8 << 20, quote, term).with_escape(escape);
    let mut records = 0u64;
    while let Some(chunk) = chunker.next_chunk()? {
        records += count_chunk(&chunk.data, quote, escape, term, count_blank);
    }
    Ok(records)
}

fn count_chunk(data: &[u8], quote: Option<u8>, escape: Option<u8>, term: u8, count_blank: bool) -> u64 {
    let q = quote.filter(|&q| memchr::memchr(q, data).is_some());
    let esc = escape.filter(|&e| Some(e) != q);
    let mut records = 0u64;
    let mut line_start = 0usize;
    let mut finish_line = |start: usize, end: usize| {
        // `end` is the terminator position; a CR before LF belongs to the terminator
        let mut e = end;
        if term == b'\n' && e > start && data[e - 1] == b'\r' {
            e -= 1;
        }
        if count_blank || e > start {
            records += 1;
        }
    };
    match q {
        None => {
            for p in memchr::memchr_iter(term, data) {
                finish_line(line_start, p);
                line_start = p + 1;
            }
        }
        Some(q) => {
            let mut inside = false;
            let mut i = 0;
            while i < data.len() {
                let b = data[i];
                if inside {
                    if Some(b) == esc {
                        i += 2;
                        continue;
                    }
                    if b == q {
                        inside = false;
                    }
                } else if b == q {
                    inside = true;
                } else if b == term {
                    finish_line(line_start, i);
                    line_start = i + 1;
                }
                i += 1;
            }
        }
    }
    if line_start < data.len() {
        records += 1;
    }
    records
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boundaries_respect_quotes() {
        let buf = b"a,b\n1,\"x\ny\"\n2,z";
        assert_eq!(last_boundary(buf, Some(b'"'), b'\n'), Some(12));
        let buf = b"a,b\n1,\"x\ny";
        assert_eq!(last_boundary(buf, Some(b'"'), b'\n'), Some(4));
        assert_eq!(last_boundary(b"no newline", Some(b'"'), b'\n'), None);
        assert_eq!(first_boundary(b"\"a\nb\",c\nd\n", Some(b'"'), b'\n'), Some(8));
    }

    #[test]
    fn chunker_reassembles_exactly() {
        let mut data = Vec::new();
        for i in 0..5000 {
            if i % 7 == 0 {
                data.extend_from_slice(format!("{i},\"multi\nline \"\"{i}\"\"\"\n").as_bytes());
            } else {
                data.extend_from_slice(format!("{i},plain\n").as_bytes());
            }
        }
        data.extend_from_slice(b"last,row");
        for size in [1, 7, 64, 1000, 1 << 20] {
            let mut chunker = Chunker::new(&data[..], size, Some(b'"'), b'\n');
            let mut out = Vec::new();
            while let Some(c) = chunker.next_chunk().unwrap() {
                assert_eq!(c.offset as usize, out.len());
                // every chunk except the last must end at a real record boundary
                if out.len() + c.data.len() < data.len() {
                    assert_eq!(*c.data.last().unwrap(), b'\n');
                    let quotes = c.data.iter().filter(|&&b| b == b'"').count();
                    assert_eq!(quotes % 2, 0);
                }
                out.extend_from_slice(&c.data);
            }
            assert_eq!(out, data);
        }
    }

    #[test]
    fn escaped_boundaries_and_hard_cuts() {
        let data = b"1,\"a \\\" b\nc\",7\n2,x\n";
        // parity alone cuts inside the quoted field, after `b` (offset 10) ...
        assert_eq!(last_boundary(&data[..16], Some(b'"'), b'\n'), Some(10));
        // ... the escape-aware scan finds the real end of the first record
        assert_eq!(last_record_end(&data[..16], Some(b'"'), Some(b'\\'), b'\n'), Some(15));
        assert_eq!(last_record_end(data, Some(b'"'), Some(b'\\'), b'\n'), Some(data.len()));
        // input without terminators is still cut once a chunk grows past max_record
        let blob = vec![b'x'; 100 << 20];
        let mut chunker = Chunker::new(&blob[..], 4 << 20, None, b'\n');
        let first = chunker.next_chunk().unwrap().unwrap();
        assert!(first.data.len() <= 128 << 20, "{}", first.data.len());
    }

    #[test]
    fn counts_records() {
        let data = b"h1,h2\n1,\"a\nb\"\n2,c\n3,d";
        assert_eq!(count_records(&data[..], Some(b'"'), None, b'\n', false).unwrap(), 4);
        assert_eq!(count_records(&b"a\nb\n"[..], None, None, b'\n', false).unwrap(), 2);
        assert_eq!(count_records(&b""[..], None, None, b'\n', false).unwrap(), 0);
        // blank lines are not records, except in single-column files
        assert_eq!(count_records(&b"a,b\r\n1,2\r\n\r\n3,4\n\n"[..], Some(b'"'), None, b'\n', false).unwrap(), 3);
        assert_eq!(count_records(&b"a\nx\n\nz\n"[..], None, None, b'\n', true).unwrap(), 4);
        // an escaped quote does not open or close a field
        let esc = b"id,t\n1,\"a \\\" b\nc\",7\n2,x\n";
        assert_eq!(count_records(&esc[..], Some(b'"'), Some(b'\\'), b'\n', false).unwrap(), 3);
    }
}
