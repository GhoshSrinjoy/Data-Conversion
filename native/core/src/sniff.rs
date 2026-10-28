//! CSV dialect detection: encoding, delimiter, quoting, terminator, header row and
//! leading preamble lines.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::tokenize::{unescape, Dialect, Term, Tokenizer};
use crate::types::{parse_bool, parse_date, parse_float, parse_int, parse_timestamp, IntParse};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CsvDialect {
    pub delimiter: String,
    pub quote: Option<String>,
    pub escape: Option<String>,
    pub double_quote: bool,
    pub terminator: String,
    pub encoding: String,
    pub bom: bool,
    pub header: bool,
    pub skip_rows: usize,
    pub columns: Vec<String>,
    pub has_quotes: bool,
    pub max_record_len: usize,
    pub sample_records: usize,
    pub confidence: f64,
}

impl CsvDialect {
    pub fn tokenizer_dialect(&self) -> Dialect {
        Dialect {
            delimiter: self.delimiter.as_bytes().first().copied().unwrap_or(b','),
            quote: self.quote.as_ref().and_then(|q| q.as_bytes().first().copied()),
            escape: self.escape.as_ref().and_then(|e| e.as_bytes().first().copied()),
            double_quote: self.double_quote,
        }
    }

    /// The byte the chunker splits on.
    pub fn split_byte(&self) -> u8 {
        if self.terminator == "\r" {
            b'\r'
        } else {
            b'\n'
        }
    }

    /// Records to skip at the start of the data (preamble lines plus the header).
    pub fn leading_records(&self) -> usize {
        self.skip_rows + usize::from(self.header)
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct SniffOptions {
    pub delimiter: Option<String>,
    pub quote: Option<String>,
    pub escape: Option<String>,
    pub encoding: Option<String>,
    pub header: Option<bool>,
    pub skip_rows: Option<usize>,
    /// File extension hint (".tsv" prefers tabs).
    pub extension: Option<String>,
}

/// Parse a one-character dialect option (`;`, `tab`, `\t`, `space`, `none`).  The
/// tokenizer works on bytes, so only single ASCII characters are accepted.
pub fn parse_char_option(text: &str) -> Result<Option<u8>> {
    Ok(match text {
        "\\t" | "tab" | "TAB" => Some(b'\t'),
        "\\n" => Some(b'\n'),
        "space" => Some(b' '),
        "none" | "" => None,
        other => match other.as_bytes() {
            [b] if b.is_ascii() => Some(*b),
            _ => {
                return Err(Error::Invalid(format!(
                    "{other:?} is not a single ASCII character (e.g. ',', ';', '|', 'tab' or 'none')"
                )))
            }
        },
    })
}

/// Detect the text encoding of a sample.  Returns (WHATWG label, has_bom).
pub fn detect_encoding(sample: &[u8], complete: bool) -> (&'static str, bool) {
    if sample.starts_with(b"\xEF\xBB\xBF") {
        return ("utf-8", true);
    }
    if sample.starts_with(b"\xFF\xFE") {
        return ("utf-16le", true);
    }
    if sample.starts_with(b"\xFE\xFF") {
        return ("utf-16be", true);
    }
    // BOM-less UTF-16: ASCII text has a NUL in every other byte.
    if sample.len() >= 64 {
        let pairs = sample.len() / 2;
        let odd_nul = sample.chunks_exact(2).filter(|p| p[1] == 0 && p[0] != 0).count();
        let even_nul = sample.chunks_exact(2).filter(|p| p[0] == 0 && p[1] != 0).count();
        if odd_nul * 10 > pairs * 7 {
            return ("utf-16le", false);
        }
        if even_nul * 10 > pairs * 7 {
            return ("utf-16be", false);
        }
    }
    let check = if complete { sample } else { trim_partial_utf8(sample) };
    if simdutf8::basic::from_utf8(check).is_ok() {
        ("utf-8", false)
    } else {
        ("windows-1252", false)
    }
}

/// Drop a multi-byte UTF-8 sequence cut off at the end of a sample.
pub fn trim_partial_utf8(sample: &[u8]) -> &[u8] {
    let n = sample.len();
    for back in 1..=3.min(n) {
        let b = sample[n - back];
        if b & 0xC0 == 0xC0 {
            let needed = if b >= 0xF0 {
                4
            } else if b >= 0xE0 {
                3
            } else {
                2
            };
            return if back < needed { &sample[..n - back] } else { sample };
        }
        if b & 0x80 == 0 {
            return sample;
        }
    }
    sample
}

pub fn to_utf8(sample: &[u8], encoding: &str) -> Vec<u8> {
    if crate::io::is_latin1(encoding) {
        let mut out = Vec::with_capacity(sample.len() + sample.len() / 8);
        crate::io::latin1_to_utf8(sample, &mut out);
        return out;
    }
    match crate::io::encoding_for(encoding) {
        Some(enc) if enc != encoding_rs::UTF_8 => {
            let (text, _, _) = enc.decode(sample);
            text.into_owned().into_bytes()
        }
        _ => sample.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(sample).to_vec(),
    }
}

/// Backslash-escaped quotes (`\"`) and no RFC 4180 doubled quotes (`""` not preceded by
/// a backslash) mean the writer escapes with backslashes.
fn detect_backslash_escape(text: &[u8], quote: Option<u8>) -> Option<u8> {
    let q = quote?;
    let mut backslashed = 0usize;
    let mut doubled = 0usize;
    let mut i = 1;
    while i < text.len() {
        if text[i] == q {
            if text[i - 1] == b'\\' {
                backslashed += 1;
            } else if i + 1 < text.len() && text[i + 1] == q && text[i - 1] != q {
                doubled += 1;
                i += 1;
            }
        }
        i += 1;
    }
    (backslashed > 0 && doubled == 0).then_some(b'\\')
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Empty,
    Number,
    Bool,
    Temporal,
    Text,
}

fn kind(v: &[u8]) -> Kind {
    if v.is_empty() {
        Kind::Empty
    } else if !matches!(parse_int(v, true), IntParse::No) || parse_float(v, true).is_some() {
        Kind::Number
    } else if parse_bool(v).is_some() {
        Kind::Bool
    } else if parse_date(v).is_some() || parse_timestamp(v).is_some() {
        Kind::Temporal
    } else {
        Kind::Text
    }
}

struct Sampled {
    rows: Vec<Vec<Vec<u8>>>,
    terms: [usize; 4],
    max_len: usize,
}

/// A field's value as the converters read it: text after a closing quote stays with it.
fn value(data: &[u8], f: &crate::tokenize::Field, d: &Dialect, scratch: &mut Vec<u8>) -> Vec<u8> {
    if f.malformed {
        crate::tokenize::malformed_value(data, f, d)
    } else {
        unescape(&data[f.start..f.end], f, d, scratch).to_vec()
    }
}

fn sample_records(text: &[u8], d: Dialect, limit: usize, complete: bool) -> Sampled {
    // In a whole file, a quote that is never closed ends with its line, as when
    // converting; in a sample it may still close beyond the sample.
    let mut t = if complete { Tokenizer::new(d) } else { Tokenizer::partial(d) };
    let mut rows = Vec::new();
    let mut terms = [0usize; 4];
    let mut max_len = 0;
    let mut scratch = Vec::new();
    t.for_each(text, |r| {
        if r.term == Term::Eof && !complete {
            // The last record was cut off by the sample boundary.
            return false;
        }
        terms[r.term.code() as usize] += 1;
        max_len = max_len.max(r.end - r.start);
        rows.push(r.fields.iter().map(|f| value(r.data, f, &d, &mut scratch)).collect());
        rows.len() < limit
    });
    Sampled { rows, terms, max_len }
}

fn field_count_score(rows: &[Vec<Vec<u8>>]) -> (f64, usize) {
    let data: Vec<usize> = rows.iter().filter(|r| !(r.len() == 1 && r[0].is_empty())).map(|r| r.len()).collect();
    if data.is_empty() {
        return (0.0, 1);
    }
    // (count, frequency) in order of first appearance; ties go to the earliest.
    let mut counts: Vec<(usize, usize)> = Vec::new();
    for &n in &data {
        match counts.iter_mut().find(|(k, _)| *k == n) {
            Some(entry) => entry.1 += 1,
            None => counts.push((n, 1)),
        }
    }
    let best = counts.iter().map(|&(_, f)| f).max().unwrap();
    let (mode, freq) = counts.iter().copied().find(|&(_, f)| f == best).unwrap();
    let consistency = freq as f64 / data.len() as f64;
    if mode < 2 {
        return (0.0, mode);
    }
    (consistency * (1.0 + (mode.min(64) as f64) / 200.0), mode)
}

/// A time of day: `H:MM`, `HH:MM:SS` with an optional fraction, then optionally `Z` or
/// a `+HH:MM` / `-HH:MM` offset.
fn is_time_of_day(v: &[u8]) -> bool {
    fn num(v: &[u8], i: &mut usize, min: usize, max: usize) -> Option<u32> {
        let s = *i;
        while *i < v.len() && *i - s < max && v[*i].is_ascii_digit() {
            *i += 1;
        }
        if *i - s < min {
            return None;
        }
        std::str::from_utf8(&v[s..*i]).ok()?.parse().ok()
    }
    let mut i = 0;
    let (Some(h), Some(b':')) = (num(v, &mut i, 1, 2), v.get(i)) else { return false };
    i += 1;
    match num(v, &mut i, 2, 2) {
        Some(m) if h <= 23 && m <= 59 => {}
        _ => return false,
    }
    if v.get(i) == Some(&b':') {
        i += 1;
        if num(v, &mut i, 2, 2).is_none_or(|s| s > 60) {
            return false;
        }
        if v.get(i) == Some(&b'.') {
            i += 1;
            if num(v, &mut i, 1, 9).is_none() {
                return false;
            }
        }
    }
    match &v[i..] {
        [] | [b'Z'] => true,
        [b'+' | b'-', a, b, b':', c, d] => [a, b, c, d].iter().all(|x| x.is_ascii_digit()),
        _ => false,
    }
}

/// Whether the sample reads as a single column whose values (after a possible
/// header) are at least 90% timestamps, dates or times of day.
fn temporal_column(text: &[u8], base: Dialect, limit: usize, complete: bool) -> bool {
    let s = sample_records(text, Dialect { delimiter: b',', ..base }, limit, complete);
    if s.rows.iter().any(|r| r.len() != 1) {
        return false;
    }
    let values: Vec<&[u8]> = s.rows.iter().skip(1).map(|r| r[0].as_slice()).filter(|v| !v.is_empty()).collect();
    let temporal =
        values.iter().filter(|v| parse_timestamp(v).is_some() || parse_date(v).is_some() || is_time_of_day(v)).count();
    !values.is_empty() && temporal * 10 >= values.len() * 9
}

/// The sampled records of a dialect, their dominant field count and the preamble
/// rows before the data (`skip_rows` when given).
fn layout(text: &[u8], d: Dialect, skip_rows: Option<usize>, complete: bool) -> (Sampled, usize, usize) {
    let sampled = sample_records(text, d, 2000, complete);
    let rows = &sampled.rows;
    // Dominant field count from the body of the sample (so preamble lines cannot
    // outvote the data); small samples use every row.
    let body = if rows.len() >= 20 { &rows[rows.len() / 4..] } else { &rows[..] };
    let (_, ncols) = field_count_score(body);
    let skip_rows = skip_rows.unwrap_or_else(|| {
        let mut k = 0;
        while k < rows.len() && k < 50 {
            let n = rows[k].len();
            let blank = n == 1 && rows[k][0].is_empty();
            if n == ncols && !blank {
                break;
            }
            if !(blank || (ncols >= 2 && n <= ncols / 2)) {
                k = 0;
                break;
            }
            k += 1;
        }
        if k >= rows.len() {
            0
        } else {
            k
        }
    });
    (sampled, ncols, skip_rows)
}

/// Whether the first of `rows` is a header with evidence: a text name over a column
/// whose values are all numbers, dates, timestamps or booleans.  (All-text data is
/// read as having a header when undecided; this asks for more.)
fn header_evidence(rows: &[Vec<Vec<u8>>]) -> bool {
    let Some(first) = rows.first() else { return false };
    let rest = &rows[1..rows.len().min(101)];
    first.iter().enumerate().any(|(j, name)| {
        let col: Vec<&Vec<u8>> = rest.iter().filter_map(|r| r.get(j)).filter(|v| !v.is_empty()).collect();
        kind(name) == Kind::Text && !col.is_empty() && col.iter().all(|v| kind(v) != Kind::Text)
    })
}

/// Detect the dialect of a CSV sample (raw bytes from the start of the file).
pub fn sniff(raw: &[u8], complete: bool, opts: &SniffOptions) -> Result<CsvDialect> {
    let (detected, bom) = detect_encoding(raw, complete);
    let encoding = opts.encoding.clone().unwrap_or_else(|| detected.to_string());
    if !crate::io::is_latin1(&encoding) && crate::io::encoding_for(&encoding).is_none() {
        return Err(Error::Invalid(format!("unknown encoding '{encoding}'")));
    }
    let text = to_utf8(if complete { raw } else { trim_partial_utf8(raw) }, &encoding);
    let quote = match &opts.quote {
        Some(q) => parse_char_option(q)?,
        None => Some(b'"'),
    };
    let has_quotes = quote.is_some_and(|q| memchr::memchr(q, &text).is_some());
    let escape = match &opts.escape {
        Some(e) => parse_char_option(e)?,
        None => detect_backslash_escape(&text, quote),
    };
    let base = Dialect { delimiter: b',', quote, escape, double_quote: escape.is_none() };
    const LIMIT: usize = 400;

    let tsv_hint = opts.extension.as_deref().is_some_and(|e| {
        let e = e.trim_start_matches('.').to_ascii_lowercase();
        e == "tsv" || e == "tab"
    });
    let (delimiter, confidence) = match opts.delimiter.as_deref().map(parse_char_option).transpose()?.flatten() {
        Some(d) => (d, 1.0),
        None => {
            let mut best = (b',', 0.0f64);
            for cand in [b',', b'\t', b';', b'|', b'^', b'~', b':'] {
                let s = sample_records(&text, Dialect { delimiter: cand, ..base }, LIMIT, complete);
                let (mut score, _) = field_count_score(&s.rows);
                if cand == b'\t' && tsv_hint {
                    score *= 1.25;
                }
                if score > best.1 + 1e-9 {
                    best = (cand, score);
                }
            }
            if best.1 == 0.0 {
                (if tsv_hint { b'\t' } else { b',' }, 0.0)
            } else {
                best
            }
        }
    };
    // A character that never occurs in the sample, for reading it as one column.
    let lone = || {
        let free = |c: &u8| memchr::memchr(*c, &text).is_none();
        if complete {
            [b',', b'\t', b';', b'|', 0x1f].into_iter().find(free)
        } else {
            Some(0x1f).filter(free)
        }
    };
    // Timestamps and times contain ':', so a single column of them is not ':'-delimited.
    let mut delimiter = delimiter;
    if delimiter == b':' && opts.delimiter.is_none() && temporal_column(&text, base, LIMIT, complete) {
        delimiter = lone().unwrap_or(b',');
    }
    if [Some(delimiter), quote, escape].iter().any(|c| matches!(c, Some(b'\n' | b'\r'))) {
        return Err(Error::Invalid("a line break cannot be the delimiter, quote or escape character".into()));
    }
    if quote == Some(delimiter) || escape == Some(delimiter) {
        return Err(Error::Invalid(format!(
            "the delimiter {:?} is also the quote or escape character; they must differ",
            delimiter as char
        )));
    }
    let mut d = Dialect { delimiter, ..base };
    let (mut sampled, mut ncols, mut skip_rows) = layout(&text, d, opts.skip_rows, complete);
    // A first row without the delimiter, taken for a one-line preamble before data that
    // has no header, is far more often the header of a single column whose values
    // contain that character (MAC addresses, URLs with ports, key:value pairs).  Unless
    // the rows below show a real header (a name over typed values), read one column:
    // every line is kept whole either way.
    if opts.delimiter.is_none()
        && opts.skip_rows.is_none()
        && skip_rows == 1
        && sampled.rows.first().is_some_and(|r| r.len() == 1 && !r[0].is_empty() && !r[0].contains(&delimiter))
        && !header_evidence(&sampled.rows[1..])
    {
        if let Some(c) = lone() {
            delimiter = c;
            d = Dialect { delimiter, ..base };
            (sampled, ncols, skip_rows) = layout(&text, d, None, complete);
        }
    }
    let rows = &sampled.rows;

    let terminator = {
        let [lf, crlf, cr, _] = sampled.terms;
        if crlf >= lf && crlf >= cr && crlf > 0 {
            "\r\n"
        } else if cr > lf && cr > crlf {
            "\r"
        } else {
            "\n"
        }
    };

    let data_rows: Vec<&Vec<Vec<u8>>> = rows.iter().skip(skip_rows).collect();
    let header = match opts.header {
        Some(h) => h,
        None => looks_like_header(&data_rows),
    };
    let width = data_rows.first().map_or(ncols, |r| r.len()).max(ncols).max(1);
    let columns = if header {
        let raw: Vec<String> = data_rows
            .first()
            .map(|r| r.iter().map(|v| String::from_utf8_lossy(v).into_owned()).collect())
            .unwrap_or_default();
        unique_names(&raw, width)
    } else {
        (1..=width).map(|i| format!("column_{i}")).collect()
    };

    Ok(CsvDialect {
        delimiter: (delimiter as char).to_string(),
        quote: quote.map(|q| (q as char).to_string()),
        escape: escape.map(|e| (e as char).to_string()),
        double_quote: escape.is_none(),
        terminator: terminator.to_string(),
        encoding,
        bom,
        header,
        skip_rows,
        columns,
        has_quotes,
        max_record_len: sampled.max_len,
        sample_records: rows.len(),
        confidence,
    })
}

fn looks_like_header(rows: &[&Vec<Vec<u8>>]) -> bool {
    let Some(first) = rows.first() else { return true };
    let rest = &rows[1..rows.len().min(101)];
    if rest.is_empty() {
        return true;
    }
    let mut score = 0i32;
    for (j, name) in first.iter().enumerate() {
        let col: Vec<&Vec<u8>> = rest.iter().filter_map(|r| r.get(j)).filter(|v| !v.is_empty()).collect();
        let k = kind(name);
        if col.is_empty() {
            continue;
        }
        let kinds: Vec<Kind> = col.iter().map(|v| kind(v)).collect();
        let typed = kinds.iter().all(|&x| x != Kind::Text);
        if k == Kind::Text && typed {
            score += 3;
        } else if k == Kind::Number && kinds.iter().all(|&x| x == Kind::Number) {
            score -= 3;
        } else if k != Kind::Text && k != Kind::Empty && typed {
            score -= 2;
        } else if col.iter().any(|v| v.as_slice() == name.as_slice()) {
            score -= 1;
        }
    }
    let mut seen = std::collections::HashSet::new();
    if first.iter().any(|n| !n.is_empty() && !seen.insert(n.as_slice())) {
        score -= 2;
    }
    score >= 0
}

/// Unique column names, kept as written (a blank one becomes `column_N`).
pub fn unique_names(raw: &[String], width: usize) -> Vec<String> {
    let mut out = Vec::with_capacity(width);
    let mut seen = std::collections::HashSet::new();
    for i in 0..width {
        // names are kept exactly as written; only a blank one is replaced
        let base = raw.get(i).cloned().unwrap_or_default();
        let base = if base.trim().is_empty() { format!("column_{}", i + 1) } else { base };
        let mut name = base.clone();
        let mut n = 2;
        while !seen.insert(name.clone()) {
            name = format!("{base}_{n}");
            n += 1;
        }
        out.push(name);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sn(text: &str) -> CsvDialect {
        sniff(text.as_bytes(), true, &SniffOptions::default()).unwrap()
    }

    #[test]
    fn delimiters() {
        assert_eq!(sn("a,b,c\n1,2,3\n4,5,6\n").delimiter, ",");
        assert_eq!(sn("a;b;c\n1,5;2,5;3\n4;5;6\n").delimiter, ";");
        assert_eq!(sn("a\tb\n1\t2\n").delimiter, "\t");
        assert_eq!(sn("a|b|c\nx|y|z\n").delimiter, "|");
        let d = sn("name,notes\n\"Smith, J\",\"a;b;c\"\n\"Doe, A\",\"x;y\"\n");
        assert_eq!(d.delimiter, ",");
        assert!(d.has_quotes);
    }

    #[test]
    fn headers() {
        assert!(sn("id,price\n1,2.5\n2,3.5\n").header);
        assert!(!sn("1,2.5\n2,3.5\n3,4.5\n").header);
        assert!(sn("name,city\nann,paris\nbob,rome\n").header);
        let d = sn(",x,x\n1,2,3\n");
        assert_eq!(d.columns, vec!["column_1", "x", "x_2"]);
    }

    #[test]
    fn preamble_and_terminators() {
        let d =
            sn("Exported report\nGenerated 2024-01-01\n\nid,value,flag\n1,2,true\n3,4,false\n5,6,true\n7,8,false\n");
        assert_eq!(d.skip_rows, 3);
        assert_eq!(d.columns, vec!["id", "value", "flag"]);
        assert_eq!(sn("a,b\r\n1,2\r\n").terminator, "\r\n");
        assert_eq!(sn("a,b\r1,2\r3,4\r").terminator, "\r");
    }

    #[test]
    fn a_column_of_times_is_not_colon_delimited() {
        let mut ts = String::from("created_at\n");
        let mut times = String::from("t\n");
        for i in 0..200 {
            ts.push_str(&format!("2024-03-01T{:02}:{:02}:00+05:30\n", i % 24, i % 60));
            times.push_str(&format!("{:02}:{:02}:{:02}.5\n", i % 24, i % 60, i % 60));
        }
        for text in [&ts, &times] {
            let d = sn(text);
            assert_eq!((d.delimiter.as_str(), d.columns.len(), d.skip_rows), (",", 1, 0), "{text:.40}");
        }
        // genuinely ':'-separated data still is
        assert_eq!(sn("user:uid:shell\nroot:0:/bin/sh\nada:1000:/bin/zsh\nbob:1001:/bin/bash\n").delimiter, ":");
        // one column whose values hold a delimiter: MAC addresses, URLs, odd times, pairs
        let mut macs = String::from("mac\n");
        let mut urls = String::from("url\n");
        let mut odd = String::from("t\n");
        let mut pairs = String::from("pair\n");
        for i in 0..50 {
            macs.push_str(&format!("0a:1b:2c:3d:4e:{i:02x}\n"));
            urls.push_str(&format!("https://h{i}.example.com:8080/p\n"));
            odd.push_str(&format!("2024-01-02T03:04:{:02}Z\n", if i == 7 { 60 } else { i % 60 }));
            pairs.push_str(&format!("key{i}:value{i}\n"));
        }
        for text in [&macs, &urls, &odd, &pairs] {
            let d = sn(text);
            assert_eq!((d.columns.len(), d.skip_rows, d.header), (1, 0, true), "{text:.30}");
        }
        // a title line above a real header is still a preamble
        let d = sn("My report\nid;name\n1;a\n2;b\n3;c\n");
        assert_eq!((d.delimiter.as_str(), d.skip_rows, d.columns.len()), (";", 1, 2));
        assert!(is_time_of_day(b"9:05") && is_time_of_day(b"23:59:60Z") && is_time_of_day(b"10:00:00-03:00"));
        assert!(!is_time_of_day(b"24:00") && !is_time_of_day(b"1:2") && !is_time_of_day(b"10:00:00+0300"));
    }

    #[test]
    fn a_quote_never_closed_ends_with_its_line() {
        // as when converting: the stray line is one record, the rest parse normally
        let d = sn("\"a,b\n1,2\n3,4\n5,6\n");
        assert_eq!((d.columns.len(), d.skip_rows), (2, 1));
        let d = sn("id,name\n1,ok\n2,\"stray\n3,x\n4,y\n");
        assert_eq!(d.columns, vec!["id", "name"]);
        assert_eq!(d.sample_records, 5);
    }

    #[test]
    fn header_names_are_kept_exactly() {
        assert_eq!(sn(" id,name ,,id\n1,a,b,c\n2,d,e,f\n").columns, vec![" id", "name ", "column_3", "id"]);
        assert_eq!(unique_names(&["a".into(), "a".into(), "  ".into()], 3), vec!["a", "a_2", "column_3"]);
        // text after a closing quote stays with the field, as when converting
        assert_eq!(sn("\"a\"x,b\n1,2\n").columns, vec!["ax", "b"]);
    }

    #[test]
    fn encodings() {
        assert_eq!(detect_encoding(b"\xEF\xBB\xBFa,b\n", true), ("utf-8", true));
        assert_eq!(detect_encoding("café,x\n".as_bytes(), true), ("utf-8", false));
        assert_eq!(detect_encoding(b"caf\xe9,x\n", true).0, "windows-1252");
        // a sample cut in the middle of a multi-byte character is still UTF-8
        let s = "ab€".as_bytes();
        assert_eq!(detect_encoding(&s[..s.len() - 1], false).0, "utf-8");
        let d = sniff(b"caf\xe9,prix\nth\xe9,2\n", true, &SniffOptions::default()).unwrap();
        assert_eq!(d.encoding, "windows-1252");
        assert_eq!(d.columns, vec!["café", "prix"]);
    }

    #[test]
    fn escapes() {
        let d = sn("a,b\n\"x \\\"y\\\"\",1\n");
        assert_eq!(d.escape.as_deref(), Some("\\"));
        assert!(!d.double_quote);
    }
}
