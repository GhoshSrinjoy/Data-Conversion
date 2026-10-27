//! Record-aligned chunking.
//!
//! A chunk always ends at a record terminator that lies outside quotes.  Quote
//! state is tracked by parity: in RFC 4180 CSV an escaped quote is written `""`,
//! so a terminator is inside a quoted field exactly when an odd number of quote
//! characters precede it in the record.  Every chunk starts at a record boundary,
//! so counting from the chunk start is enough.

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
        if (total - after) % 2 == 0 {
            return Some(nl + 1);
        }
        end = nl;
    }
    None
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
        if quotes % 2 == 0 {
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
    term: u8,
    carry: Vec<u8>,
    offset: u64,
    index: usize,
    eof: bool,
    /// Records larger than this are not searched for a quote-balanced boundary; the
    /// chunk is cut at the last terminator instead (protects memory against a stray
    /// unbalanced quote).
    max_record: usize,
}

impl<R: Read> Chunker<R> {
    pub fn new(reader: R, chunk_size: usize, quote: Option<u8>, term: u8) -> Self {
        Chunker {
            reader,
            chunk_size: chunk_size.max(1),
            quote,
            term,
            carry: Vec::new(),
            offset: 0,
            index: 0,
            eof: false,
            max_record: (chunk_size.max(1) * 8).max(64 << 20),
        }
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
                let mut cut = last_boundary(&self.carry, self.quote, self.term);
                if cut.is_none() && self.carry.len() >= self.max_record {
                    cut = memchr::memrchr(self.term, &self.carry).map(|i| i + 1);
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
/// terminator counts too.  Runs at memory bandwidth.
pub fn count_records<R: Read>(reader: R, quote: Option<u8>, term: u8) -> Result<u64> {
    let mut chunker = Chunker::new(reader, 8 << 20, quote, term);
    let mut records = 0u64;
    while let Some(chunk) = chunker.next_chunk()? {
        let data = &chunk.data;
        let n = count_byte(data, term) as u64;
        let inside = match quote {
            Some(q) if memchr::memchr(q, data).is_some() => count_quoted_terminators(data, q, term),
            _ => 0,
        };
        records += n - inside;
        if data.last() != Some(&term) {
            records += 1;
        }
    }
    Ok(records)
}

fn count_quoted_terminators(data: &[u8], q: u8, term: u8) -> u64 {
    let mut inside = false;
    let mut count = 0u64;
    for &b in data {
        if b == q {
            inside = !inside;
        } else if b == term && inside {
            count += 1;
        }
    }
    count
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
    fn counts_records() {
        let data = b"h1,h2\n1,\"a\nb\"\n2,c\n3,d";
        assert_eq!(count_records(&data[..], Some(b'"'), b'\n').unwrap(), 4);
        assert_eq!(count_records(&b"a\nb\n"[..], None, b'\n').unwrap(), 2);
        assert_eq!(count_records(&b""[..], None, b'\n').unwrap(), 0);
    }
}
