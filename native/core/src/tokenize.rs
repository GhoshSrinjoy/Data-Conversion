//! A fast, lenient, quote-aware CSV tokenizer.
//!
//! It never allocates per field: fields are reported as offsets into the chunk,
//! together with exactly enough information to rebuild the record's original bytes
//! (quoting and terminator), which the DCZ codec relies on.

use memchr::{memchr, memchr2, memchr3};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Dialect {
    pub delimiter: u8,
    pub quote: Option<u8>,
    /// Escape character inside quoted fields (e.g. `\` for `\"`); None for RFC 4180.
    pub escape: Option<u8>,
    /// Whether a doubled quote inside a quoted field means a literal quote.
    pub double_quote: bool,
}

impl Default for Dialect {
    fn default() -> Self {
        Dialect { delimiter: b',', quote: Some(b'"'), escape: None, double_quote: true }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Field {
    /// Content start (just after the opening quote when quoted).
    pub start: usize,
    /// Content end (just before the closing quote when quoted).
    pub end: usize,
    /// End of the field's raw text, i.e. the offset of the following delimiter or
    /// terminator.  Differs from `end + 1` only for malformed quoted fields.
    pub raw_end: usize,
    pub quoted: bool,
    /// The content contains escape sequences and must be unescaped to get the value.
    pub escaped: bool,
    /// Quoted field with text after the closing quote, or an unterminated quote.
    pub malformed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Term {
    Lf,
    CrLf,
    Cr,
    /// The record runs to the end of the data without a terminator.
    Eof,
}

impl Term {
    pub fn bytes(self) -> &'static [u8] {
        match self {
            Term::Lf => b"\n",
            Term::CrLf => b"\r\n",
            Term::Cr => b"\r",
            Term::Eof => b"",
        }
    }

    pub fn code(self) -> u8 {
        match self {
            Term::Lf => 0,
            Term::CrLf => 1,
            Term::Cr => 2,
            Term::Eof => 3,
        }
    }

    pub fn from_code(code: u8) -> Option<Term> {
        Some(match code {
            0 => Term::Lf,
            1 => Term::CrLf,
            2 => Term::Cr,
            3 => Term::Eof,
            _ => return None,
        })
    }
}

pub struct Record<'a> {
    pub data: &'a [u8],
    pub start: usize,
    pub end: usize,
    pub fields: &'a [Field],
    pub term: Term,
    /// Every field is either unquoted or cleanly quoted, so the record's bytes are
    /// exactly the fields joined by the delimiter plus the terminator.
    pub regular: bool,
}

impl<'a> Record<'a> {
    #[inline]
    pub fn raw(&self) -> &'a [u8] {
        &self.data[self.start..self.end]
    }

    /// Field content as it appears in the file (escape sequences intact).
    #[inline]
    pub fn content(&self, i: usize) -> &'a [u8] {
        let f = &self.fields[i];
        &self.data[f.start..f.end]
    }

    /// A blank line: a single empty unquoted field.
    #[inline]
    pub fn is_blank(&self) -> bool {
        self.fields.len() == 1 && !self.fields[0].quoted && self.fields[0].start == self.fields[0].end
    }
}

pub struct Tokenizer {
    pub dialect: Dialect,
    fields: Vec<Field>,
}

impl Tokenizer {
    pub fn new(dialect: Dialect) -> Self {
        Tokenizer { dialect, fields: Vec::with_capacity(64) }
    }

    /// Visit every record in `data`.  The visitor returns false to stop early.
    pub fn for_each<F>(&mut self, data: &[u8], mut visit: F)
    where
        F: FnMut(&Record<'_>) -> bool,
    {
        let d = self.dialect;
        let n = data.len();
        let mut pos = 0usize;
        while pos < n {
            let start = pos;
            self.fields.clear();
            let mut regular = true;
            let term;
            loop {
                if pos < n && Some(data[pos]) == d.quote {
                    // Quoted field.
                    let q = data[pos];
                    let content_start = pos + 1;
                    let mut i = content_start;
                    let mut escaped = false;
                    let closing = loop {
                        let hit = match d.escape {
                            Some(e) => memchr2(q, e, &data[i..]),
                            None => memchr(q, &data[i..]),
                        };
                        match hit {
                            None => break None,
                            Some(k) => {
                                let j = i + k;
                                if Some(data[j]) == d.escape && data[j] != q {
                                    escaped = true;
                                    i = j + 2;
                                    if i > n {
                                        break None;
                                    }
                                } else if d.double_quote && j + 1 < n && data[j + 1] == q {
                                    escaped = true;
                                    i = j + 2;
                                } else {
                                    break Some(j);
                                }
                            }
                        }
                    };
                    match closing {
                        None => {
                            // Unterminated quote: the field swallows the rest of the data.
                            self.fields.push(Field {
                                start: content_start.min(n),
                                end: n,
                                raw_end: n,
                                quoted: true,
                                escaped,
                                malformed: true,
                            });
                            regular = false;
                            pos = n;
                            term = Term::Eof;
                            break;
                        }
                        Some(j) => {
                            let after = j + 1;
                            if after < n && data[after] != d.delimiter && data[after] != b'\n' && data[after] != b'\r' {
                                // Text after the closing quote: keep it with the field.
                                let k = memchr3(d.delimiter, b'\n', b'\r', &data[after..]).map_or(n, |k| after + k);
                                self.fields.push(Field {
                                    start: content_start,
                                    end: j,
                                    raw_end: k,
                                    quoted: true,
                                    escaped,
                                    malformed: true,
                                });
                                regular = false;
                                pos = k;
                            } else {
                                self.fields.push(Field {
                                    start: content_start,
                                    end: j,
                                    raw_end: after,
                                    quoted: true,
                                    escaped,
                                    malformed: false,
                                });
                                pos = after;
                            }
                        }
                    }
                } else {
                    let k = memchr3(d.delimiter, b'\n', b'\r', &data[pos..]).map_or(n, |k| pos + k);
                    self.fields.push(Field {
                        start: pos,
                        end: k,
                        raw_end: k,
                        quoted: false,
                        escaped: false,
                        malformed: false,
                    });
                    pos = k;
                }
                // `pos` is at a delimiter, a terminator, or the end of the data.
                if pos >= n {
                    term = Term::Eof;
                    break;
                }
                let b = data[pos];
                if b == d.delimiter {
                    pos += 1;
                    if pos >= n {
                        // Trailing delimiter at the very end: one more empty field.
                        self.fields.push(Field { start: n, end: n, raw_end: n, ..Field::default() });
                        term = Term::Eof;
                        break;
                    }
                    continue;
                }
                if b == b'\n' {
                    pos += 1;
                    term = Term::Lf;
                } else if pos + 1 < n && data[pos + 1] == b'\n' {
                    pos += 2;
                    term = Term::CrLf;
                } else {
                    pos += 1;
                    term = Term::Cr;
                }
                break;
            }
            let record = Record { data, start, end: pos, fields: &self.fields, term, regular };
            if !visit(&record) {
                return;
            }
        }
    }
}

/// Unescape a quoted field's content into `buf` and return the value bytes.  Content
/// without escapes is returned as-is without copying.
pub fn unescape<'b>(content: &'b [u8], field: &Field, d: &Dialect, buf: &'b mut Vec<u8>) -> &'b [u8] {
    if !field.escaped {
        return content;
    }
    buf.clear();
    let q = d.quote.unwrap_or(b'"');
    let mut i = 0;
    while i < content.len() {
        let b = content[i];
        if Some(b) == d.escape && b != q && i + 1 < content.len() {
            buf.push(content[i + 1]);
            i += 2;
        } else if b == q && d.double_quote && i + 1 < content.len() && content[i + 1] == q {
            buf.push(q);
            i += 2;
        } else {
            buf.push(b);
            i += 1;
        }
    }
    buf
}

/// The value of a malformed field the way lenient CSV readers see it: the quoted part
/// (unescaped) followed by whatever came after the closing quote.
pub fn malformed_value(data: &[u8], field: &Field, d: &Dialect) -> Vec<u8> {
    let mut scratch = Vec::new();
    let mut out = unescape(&data[field.start..field.end], field, d, &mut scratch).to_vec();
    let tail_start = (field.end + 1).min(field.raw_end);
    out.extend_from_slice(&data[tail_start..field.raw_end]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn records(data: &[u8], d: Dialect) -> Vec<(Vec<String>, Term, bool)> {
        let mut out = Vec::new();
        let mut t = Tokenizer::new(d);
        t.for_each(data, |r| {
            let mut scratch = Vec::new();
            let vals = r
                .fields
                .iter()
                .map(|f| {
                    if f.malformed {
                        String::from_utf8_lossy(&malformed_value(data, f, &d)).into_owned()
                    } else {
                        String::from_utf8_lossy(unescape(&data[f.start..f.end], f, &d, &mut scratch)).into_owned()
                    }
                })
                .collect();
            out.push((vals, r.term, r.regular));
            true
        });
        out
    }

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn basic_and_terminators() {
        let r = records(b"a,b,c\r\n1,,3\n\n4,5,6", Dialect::default());
        assert_eq!(r[0], (s(&["a", "b", "c"]), Term::CrLf, true));
        assert_eq!(r[1], (s(&["1", "", "3"]), Term::Lf, true));
        assert_eq!(r[2], (s(&[""]), Term::Lf, true));
        assert_eq!(r[3], (s(&["4", "5", "6"]), Term::Eof, true));
    }

    #[test]
    fn quotes_escapes_and_newlines() {
        let r = records(b"\"x,y\",\"he said \"\"hi\"\"\",\"line1\nline2\"\n\"\",plain\n", Dialect::default());
        assert_eq!(r[0].0, s(&["x,y", "he said \"hi\"", "line1\nline2"]));
        assert!(r[0].2);
        assert_eq!(r[1].0, s(&["", "plain"]));
    }

    #[test]
    fn trailing_delimiter_and_lone_cr() {
        let r = records(b"a,b,\rc,d,", Dialect::default());
        assert_eq!(r[0], (s(&["a", "b", ""]), Term::Cr, true));
        assert_eq!(r[1], (s(&["c", "d", ""]), Term::Eof, true));
    }

    #[test]
    fn malformed_is_lenient_and_irregular() {
        let r = records(b"\"abc\"def,x\n\"open,y\n", Dialect::default());
        assert_eq!(r[0].0, s(&["abcdef", "x"]));
        assert!(!r[0].2);
        assert_eq!(r[1].0, s(&["open,y\n"]));
        assert_eq!(r[1].1, Term::Eof);
        assert!(!r[1].2);
    }

    #[test]
    fn backslash_escape_dialect() {
        let d = Dialect { escape: Some(b'\\'), double_quote: false, ..Dialect::default() };
        let r = records(b"\"a\\\"b\",c\n", d);
        assert_eq!(r[0].0, s(&["a\"b", "c"]));
    }

    #[test]
    fn regular_records_rebuild_exactly() {
        let data = b"id,name\r\n1,\"a,b\"\n2,\"say \"\"x\"\"\"\r\n3,plain";
        let mut t = Tokenizer::new(Dialect::default());
        let mut rebuilt = Vec::new();
        t.for_each(data, |r| {
            assert!(r.regular);
            for (i, f) in r.fields.iter().enumerate() {
                if i > 0 {
                    rebuilt.push(b',');
                }
                if f.quoted {
                    rebuilt.push(b'"');
                    rebuilt.extend_from_slice(r.content(i));
                    rebuilt.push(b'"');
                } else {
                    rebuilt.extend_from_slice(r.content(i));
                }
            }
            rebuilt.extend_from_slice(r.term.bytes());
            assert_eq!(&rebuilt[r.start..], r.raw());
            true
        });
        assert_eq!(rebuilt, data);
    }
}
