//! Column encoders for delimited text.  Every encoder reproduces each cell's exact
//! bytes; cells an encoder cannot represent are stored verbatim as exceptions.
//!
//! ```text
//! column_meta := enc:u8 quoting:u8 exceptions:varint params
//!   quoting: 0 = no cell quoted, 1 = every cell quoted, 2 = per-cell bitmap (aux stream)
//! enc 0 RAW       streams: lengths:int_array, data
//! enc 1 CONST     params: len:varint bytes
//! enc 2 DICT      params: entries:varint      streams: lengths:int_array, data, indices:int_array
//! enc 3 INT       params: has_empty:u8        streams: values:int_array
//! enc 4 DECIMAL   params: has_empty:u8 scale:u8 (255 = per-value)
//!                 streams: [scales:int_array] mantissas:int_array
//! enc 5 TEMPLATE  params: has_empty:u8 parts:varint (kind:u8 literal(len,bytes) | width:u8)
//!                 streams: one int_array per numeric field
//! ```
//! The aux stream (present when quoting = 2, exceptions > 0 or has_empty = 1) holds,
//! in order: quote bitmap, empty-cell bitmap, exception positions (int_array),
//! exception lengths (int_array), exception bytes.

use ahash::AHashMap;

use super::ints::{self, put_varint, Cursor};
use crate::error::{Error, Result};

pub const RAW: u8 = 0;
pub const CONST: u8 = 1;
pub const DICT: u8 = 2;
pub const INT: u8 = 3;
pub const DECIMAL: u8 = 4;
pub const TEMPLATE: u8 = 5;

pub struct EncodedColumn {
    pub meta: Vec<u8>,
    /// Streams in decode order; the flag marks fixed-width numeric data.
    pub streams: Vec<Vec<u8>>,
}

/// `-?(0|[1-9][0-9]{0,17})`, excluding "-0" (which would lose its sign).
#[inline]
pub fn canonical_int(v: &[u8]) -> Option<i64> {
    let (neg, d) = match v.first()? {
        b'-' => (true, &v[1..]),
        _ => (false, v),
    };
    if d.is_empty() || d.len() > 18 || (d.len() > 1 && d[0] == b'0') {
        return None;
    }
    let mut acc: i64 = 0;
    for &b in d {
        let x = b.wrapping_sub(b'0');
        if x > 9 {
            return None;
        }
        acc = acc * 10 + x as i64;
    }
    if neg && acc == 0 {
        return None;
    }
    Some(if neg { -acc } else { acc })
}

/// `-?(0|[1-9][0-9]*)(\.[0-9]+)?` with at most 18 digits, as (mantissa, scale).
#[inline]
pub fn canonical_decimal(v: &[u8]) -> Option<(i64, u8)> {
    let (neg, body) = match v.first()? {
        b'-' => (true, &v[1..]),
        _ => (false, v),
    };
    let (ip, fp) = match memchr::memchr(b'.', body) {
        Some(p) => (&body[..p], Some(&body[p + 1..])),
        None => (body, None),
    };
    if ip.is_empty() || (ip.len() > 1 && ip[0] == b'0') {
        return None;
    }
    let fp = match fp {
        Some([]) => return None,
        Some(f) => f,
        None => &[][..],
    };
    if ip.len() + fp.len() > 18 {
        return None;
    }
    let mut acc: i64 = 0;
    for &b in ip.iter().chain(fp) {
        let x = b.wrapping_sub(b'0');
        if x > 9 {
            return None;
        }
        acc = acc * 10 + x as i64;
    }
    if neg && acc == 0 {
        return None;
    }
    Some((if neg { -acc } else { acc }, fp.len() as u8))
}

pub fn write_decimal(out: &mut Vec<u8>, m: i64, scale: u8) {
    if m < 0 {
        out.push(b'-');
    }
    let mut buf = itoa::Buffer::new();
    let digits = buf.format(m.unsigned_abs()).as_bytes();
    let s = scale as usize;
    if s == 0 {
        out.extend_from_slice(digits);
        return;
    }
    if digits.len() <= s {
        out.push(b'0');
        out.push(b'.');
        out.extend(std::iter::repeat_n(b'0', s - digits.len()));
        out.extend_from_slice(digits);
    } else {
        let split = digits.len() - s;
        out.extend_from_slice(&digits[..split]);
        out.push(b'.');
        out.extend_from_slice(&digits[split..]);
    }
}

// ---------------------------------------------------------------------------------
// Templates
// ---------------------------------------------------------------------------------

const FIELD: u8 = 0x01;

/// Signature of a cell: digit runs replaced by a marker byte.
fn signature(v: &[u8], out: &mut Vec<u8>) -> bool {
    out.clear();
    let mut i = 0;
    let mut fields = 0;
    while i < v.len() {
        if v[i].is_ascii_digit() {
            while i < v.len() && v[i].is_ascii_digit() {
                i += 1;
            }
            out.push(FIELD);
            fields += 1;
        } else {
            if v[i] == FIELD {
                return false;
            }
            out.push(v[i]);
            i += 1;
        }
    }
    fields > 0 && fields <= 16
}

#[derive(Clone, Debug, PartialEq)]
enum Part {
    Literal(Vec<u8>),
    /// Numeric field: fixed zero-padded width, or 0 for canonical (no leading zeros).
    Field(u8),
}

fn parse_signature(sig: &[u8]) -> Vec<Part> {
    let mut parts = Vec::new();
    let mut lit = Vec::new();
    for &b in sig {
        if b == FIELD {
            if !lit.is_empty() {
                parts.push(Part::Literal(std::mem::take(&mut lit)));
            }
            parts.push(Part::Field(0));
        } else {
            lit.push(b);
        }
    }
    if !lit.is_empty() {
        parts.push(Part::Literal(lit));
    }
    parts
}

/// Digit runs of a cell matching `parts`, or None.
fn split_fields<'a>(v: &'a [u8], parts: &[Part], out: &mut Vec<&'a [u8]>) -> bool {
    out.clear();
    let mut i = 0;
    for (k, p) in parts.iter().enumerate() {
        match p {
            Part::Literal(l) => {
                if !v[i..].starts_with(l) {
                    return false;
                }
                i += l.len();
            }
            Part::Field(_) => {
                let s = i;
                while i < v.len() && v[i].is_ascii_digit() {
                    i += 1;
                }
                if i == s || i - s > 18 {
                    return false;
                }
                // a field must not be followed directly by another digit-starting part
                if let Some(Part::Field(_)) = parts.get(k + 1) {
                    return false;
                }
                out.push(&v[s..i]);
            }
        }
    }
    i == v.len()
}

fn parse_digits(d: &[u8]) -> i64 {
    d.iter().fold(0i64, |acc, &b| acc * 10 + (b - b'0') as i64)
}

fn write_field(out: &mut Vec<u8>, value: i64, width: u8) {
    let mut buf = itoa::Buffer::new();
    let s = buf.format(value).as_bytes();
    for _ in s.len()..width as usize {
        out.push(b'0');
    }
    out.extend_from_slice(s);
}

// ---------------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------------

struct Aux {
    quoting: u8,
    has_empty: bool,
    exceptions: Vec<(usize, Vec<u8>)>,
}

fn quoting_mode(quoted: &[bool]) -> u8 {
    if quoted.iter().all(|&q| !q) {
        0
    } else if quoted.iter().all(|&q| q) {
        1
    } else {
        2
    }
}

fn build_aux(aux: &Aux, quoted: &[bool], empty: Option<&[bool]>) -> Vec<u8> {
    let mut out = Vec::new();
    if aux.quoting == 2 {
        ints::put_bitmap(&mut out, quoted);
    }
    if aux.has_empty {
        ints::put_bitmap(&mut out, empty.expect("empty bitmap"));
    }
    if !aux.exceptions.is_empty() {
        let positions: Vec<i64> = aux.exceptions.iter().map(|(p, _)| *p as i64).collect();
        let lengths: Vec<i64> = aux.exceptions.iter().map(|(_, b)| b.len() as i64).collect();
        ints::encode(&mut out, &positions);
        ints::encode(&mut out, &lengths);
        for (_, b) in &aux.exceptions {
            out.extend_from_slice(b);
        }
    }
    out
}

fn needs_aux(aux: &Aux) -> bool {
    aux.quoting == 2 || aux.has_empty || !aux.exceptions.is_empty()
}

fn finish(
    enc: u8,
    aux: Aux,
    params: Vec<u8>,
    mut main: Vec<Vec<u8>>,
    quoted: &[bool],
    empty: Option<&[bool]>,
) -> EncodedColumn {
    let mut meta = vec![enc, aux.quoting];
    put_varint(&mut meta, aux.exceptions.len() as u64);
    meta.push(aux.has_empty as u8);
    meta.extend_from_slice(&params);
    let mut streams = Vec::new();
    if needs_aux(&aux) {
        streams.push(build_aux(&aux, quoted, empty));
    }
    streams.append(&mut main);
    EncodedColumn { meta, streams }
}

fn encode_raw(cells: &[&[u8]], quoted: &[bool]) -> EncodedColumn {
    let lengths: Vec<i64> = cells.iter().map(|c| c.len() as i64).collect();
    let mut data = Vec::with_capacity(lengths.iter().sum::<i64>() as usize);
    for c in cells {
        data.extend_from_slice(c);
    }
    let aux = Aux { quoting: quoting_mode(quoted), has_empty: false, exceptions: Vec::new() };
    finish(RAW, aux, Vec::new(), vec![ints::encode_to_vec(&lengths), data], quoted, None)
}

fn encode_dict(cells: &[&[u8]], quoted: &[bool], limit: usize) -> Option<EncodedColumn> {
    let mut map: AHashMap<&[u8], i64> = AHashMap::with_capacity(1024);
    let mut entries: Vec<&[u8]> = Vec::new();
    let mut indices = Vec::with_capacity(cells.len());
    for &c in cells {
        let next = entries.len() as i64;
        let idx = *map.entry(c).or_insert_with(|| {
            entries.push(c);
            next
        });
        if entries.len() > limit {
            return None;
        }
        indices.push(idx);
    }
    let lengths: Vec<i64> = entries.iter().map(|e| e.len() as i64).collect();
    let mut data = Vec::new();
    for e in &entries {
        data.extend_from_slice(e);
    }
    let mut params = Vec::new();
    put_varint(&mut params, entries.len() as u64);
    let aux = Aux { quoting: quoting_mode(quoted), has_empty: false, exceptions: Vec::new() };
    Some(finish(
        DICT,
        aux,
        params,
        vec![ints::encode_to_vec(&lengths), data, ints::encode_to_vec(&indices)],
        quoted,
        None,
    ))
}

fn exception_budget(n: usize) -> usize {
    n / 50 + 2
}

fn encode_int(cells: &[&[u8]], quoted: &[bool]) -> Option<EncodedColumn> {
    let mut values = Vec::with_capacity(cells.len());
    let mut empty = vec![false; cells.len()];
    let mut exceptions = Vec::new();
    let budget = exception_budget(cells.len());
    for (i, &c) in cells.iter().enumerate() {
        if c.is_empty() {
            empty[i] = true;
        } else if let Some(v) = canonical_int(c) {
            values.push(v);
        } else {
            exceptions.push((i, c.to_vec()));
            if exceptions.len() > budget {
                return None;
            }
        }
    }
    if values.is_empty() {
        return None;
    }
    let has_empty = empty.iter().any(|&e| e);
    let aux = Aux { quoting: quoting_mode(quoted), has_empty, exceptions };
    Some(finish(INT, aux, Vec::new(), vec![ints::encode_to_vec(&values)], quoted, Some(&empty)))
}

fn encode_decimal(cells: &[&[u8]], quoted: &[bool]) -> Option<EncodedColumn> {
    let mut mantissas = Vec::with_capacity(cells.len());
    let mut scales = Vec::with_capacity(cells.len());
    let mut empty = vec![false; cells.len()];
    let mut exceptions = Vec::new();
    let budget = exception_budget(cells.len());
    for (i, &c) in cells.iter().enumerate() {
        if c.is_empty() {
            empty[i] = true;
        } else if let Some((m, s)) = canonical_decimal(c) {
            mantissas.push(m);
            scales.push(s as i64);
        } else {
            exceptions.push((i, c.to_vec()));
            if exceptions.len() > budget {
                return None;
            }
        }
    }
    if mantissas.is_empty() || scales.iter().all(|&s| s == 0) {
        return None;
    }
    let constant = scales.iter().all(|&s| s == scales[0]);
    let has_empty = empty.iter().any(|&e| e);
    let mut params = Vec::new();
    let mut main = Vec::new();
    if constant {
        params.push(scales[0] as u8);
    } else {
        params.push(255);
        main.push(ints::encode_to_vec(&scales));
    }
    main.push(ints::encode_to_vec(&mantissas));
    let aux = Aux { quoting: quoting_mode(quoted), has_empty, exceptions };
    Some(finish(DECIMAL, aux, params, main, quoted, Some(&empty)))
}

fn encode_template(cells: &[&[u8]], quoted: &[bool]) -> Option<EncodedColumn> {
    // Dominant signature over a sample of non-empty cells.
    let mut counts: AHashMap<Vec<u8>, usize> = AHashMap::new();
    let mut sig = Vec::new();
    let mut sampled = 0usize;
    for &c in cells.iter().filter(|c| !c.is_empty()).take(4096) {
        sampled += 1;
        if signature(c, &mut sig) {
            *counts.entry(sig.clone()).or_insert(0) += 1;
        }
    }
    // Deterministic: highest count, then the smallest signature.
    let (best, hits) = counts.into_iter().max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(&a.0)))?;
    if hits * 10 < sampled * 9 {
        return None;
    }
    let mut parts = parse_signature(&best);
    let nfields = parts.iter().filter(|p| matches!(p, Part::Field(_))).count();
    // Gather field digit runs for every matching cell.
    let mut runs: Vec<Vec<&[u8]>> = vec![Vec::with_capacity(cells.len()); nfields];
    let mut matched = vec![false; cells.len()];
    let mut empty = vec![false; cells.len()];
    let mut scratch = Vec::new();
    for (i, &c) in cells.iter().enumerate() {
        if c.is_empty() {
            empty[i] = true;
        } else if split_fields(c, &parts, &mut scratch) {
            matched[i] = true;
            for (k, d) in scratch.iter().enumerate() {
                runs[k].push(d);
            }
        }
    }
    // Field formats: fixed width when (nearly) every run has one width, else canonical.
    let mut widths = Vec::with_capacity(nfields);
    for field_runs in &runs {
        let mut freq: AHashMap<usize, usize> = AHashMap::new();
        for d in field_runs {
            *freq.entry(d.len()).or_insert(0) += 1;
        }
        let (w, n) = freq.into_iter().max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(&a.0))).unwrap_or((0, 0));
        let canonical = field_runs.iter().filter(|d| d.len() == 1 || d[0] != b'0').count();
        widths.push(if n * 100 >= field_runs.len() * 99 && n >= canonical { w as u8 } else { 0 });
    }
    let mut k = 0;
    for p in parts.iter_mut() {
        if let Part::Field(w) = p {
            *w = widths[k];
            k += 1;
        }
    }
    // Final pass: cells whose fields violate the formats become exceptions.
    let mut fields: Vec<Vec<i64>> = vec![Vec::with_capacity(cells.len()); nfields];
    let mut exceptions = Vec::new();
    let budget = exception_budget(cells.len());
    let mut cursor = 0usize;
    for (i, &c) in cells.iter().enumerate() {
        if empty[i] {
            continue;
        }
        let ok =
            matched[i] && {
                let row: Vec<&[u8]> = (0..nfields).map(|f| runs[f][cursor]).collect();
                cursor += 1;
                let fits = row.iter().zip(&widths).all(|(d, &w)| {
                    if w > 0 {
                        d.len() == w as usize
                    } else {
                        d.len() == 1 || d[0] != b'0'
                    }
                });
                if fits {
                    for (f, d) in row.iter().enumerate() {
                        fields[f].push(parse_digits(d));
                    }
                }
                fits
            };
        if !ok {
            exceptions.push((i, c.to_vec()));
            if exceptions.len() > budget {
                return None;
            }
        }
    }
    let mut params = Vec::new();
    put_varint(&mut params, parts.len() as u64);
    for p in &parts {
        match p {
            Part::Literal(l) => {
                params.push(0);
                put_varint(&mut params, l.len() as u64);
                params.extend_from_slice(l);
            }
            Part::Field(w) => {
                params.push(1);
                params.push(*w);
            }
        }
    }
    let has_empty = empty.iter().any(|&e| e);
    let aux = Aux { quoting: quoting_mode(quoted), has_empty, exceptions };
    let main = fields.iter().map(|f| ints::encode_to_vec(f)).collect();
    Some(finish(TEMPLATE, aux, params, main, quoted, Some(&empty)))
}

fn encode_const(cells: &[&[u8]], quoted: &[bool]) -> Option<EncodedColumn> {
    let first = *cells.first()?;
    if !cells.iter().all(|c| *c == first) {
        return None;
    }
    let mut params = Vec::new();
    put_varint(&mut params, first.len() as u64);
    params.extend_from_slice(first);
    let aux = Aux { quoting: quoting_mode(quoted), has_empty: false, exceptions: Vec::new() };
    Some(finish(CONST, aux, params, Vec::new(), quoted, None))
}

/// All applicable encodings, most specific first.
pub fn candidates(cells: &[&[u8]], quoted: &[bool], exhaustive: bool) -> Vec<EncodedColumn> {
    if let Some(c) = encode_const(cells, quoted) {
        return vec![c];
    }
    let mut out = Vec::new();
    if let Some(c) = encode_int(cells, quoted) {
        out.push(c);
        if !exhaustive {
            return out;
        }
    } else if let Some(c) = encode_decimal(cells, quoted) {
        out.push(c);
        if !exhaustive {
            return out;
        }
    }
    if let Some(c) = encode_template(cells, quoted) {
        out.push(c);
        if !exhaustive {
            return out;
        }
    }
    let n = cells.len();
    let limit = if exhaustive { n / 2 + 1 } else { n / 3 + 1 };
    if let Some(c) = encode_dict(cells, quoted, limit.min(1 << 20)) {
        out.push(c);
        if !exhaustive {
            return out;
        }
    }
    out.push(encode_raw(cells, quoted));
    out
}

// ---------------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------------

/// Decoded cells of one column: concatenated bytes, end offsets, quote flags.
pub struct Cells {
    pub data: Vec<u8>,
    pub ends: Vec<u32>,
    pub quoted: Vec<bool>,
}

impl Cells {
    #[inline]
    pub fn get(&self, i: usize) -> &[u8] {
        let start = if i == 0 { 0 } else { self.ends[i - 1] as usize };
        &self.data[start..self.ends[i] as usize]
    }
}

pub fn decode_column(meta: &mut Cursor<'_>, streams: &mut dyn FnMut() -> Result<Vec<u8>>, n: usize) -> Result<Cells> {
    let enc = meta.u8()?;
    let quoting = meta.u8()?;
    let n_exc = meta.usize()?;
    let has_empty = meta.u8()? != 0;
    if n_exc > n {
        return Err(ints::truncated());
    }
    let mut quoted = vec![quoting == 1; n];
    let mut empty = vec![false; n];
    let mut exceptions: Vec<(usize, Vec<u8>)> = Vec::new();
    if quoting == 2 || has_empty || n_exc > 0 {
        let aux = streams()?;
        let mut c = Cursor::new(&aux);
        if quoting == 2 {
            quoted = ints::get_bitmap(&mut c, n)?;
        }
        if has_empty {
            empty = ints::get_bitmap(&mut c, n)?;
        }
        if n_exc > 0 {
            let positions = ints::decode(&mut c)?;
            let lengths = ints::decode(&mut c)?;
            if positions.len() != n_exc || lengths.len() != n_exc {
                return Err(ints::truncated());
            }
            for (p, l) in positions.into_iter().zip(lengths) {
                if p < 0 || p as usize >= n || l < 0 {
                    return Err(ints::truncated());
                }
                exceptions.push((p as usize, c.bytes(l as usize)?.to_vec()));
            }
        }
    }
    let mut data = Vec::new();
    let mut ends = Vec::with_capacity(n);
    let mut exc = exceptions.into_iter().peekable();
    // Emits cells in order, consulting empties and exceptions; `next` produces the
    // encoder-specific cell bytes.
    let mut emit =
        |data: &mut Vec<u8>, ends: &mut Vec<u32>, next: &mut dyn FnMut(&mut Vec<u8>) -> Result<()>| -> Result<()> {
            for (i, &is_empty) in empty.iter().enumerate() {
                if is_empty {
                } else if exc.peek().is_some_and(|(p, _)| *p == i) {
                    data.extend_from_slice(&exc.next().unwrap().1);
                } else {
                    next(data)?;
                }
                if data.len() > u32::MAX as usize {
                    return Err(Error::Format("DCZ block too large".into()));
                }
                ends.push(data.len() as u32);
            }
            Ok(())
        };
    match enc {
        RAW => {
            let lengths = ints::decode_all(&streams()?)?;
            let blob = streams()?;
            let mut pos = 0usize;
            let mut li = lengths.into_iter();
            emit(&mut data, &mut ends, &mut |out| {
                let l = li.next().ok_or_else(ints::truncated)? as usize;
                let s = blob.get(pos..pos + l).ok_or_else(ints::truncated)?;
                out.extend_from_slice(s);
                pos += l;
                Ok(())
            })?;
        }
        CONST => {
            let len = meta.usize()?;
            let value = meta.bytes(len)?.to_vec();
            emit(&mut data, &mut ends, &mut |out| {
                out.extend_from_slice(&value);
                Ok(())
            })?;
        }
        DICT => {
            let entries = meta.usize()?;
            let lengths = ints::decode_all(&streams()?)?;
            let blob = streams()?;
            let indices = ints::decode_all(&streams()?)?;
            if lengths.len() != entries {
                return Err(ints::truncated());
            }
            let mut starts = Vec::with_capacity(entries + 1);
            let mut pos = 0usize;
            for &l in &lengths {
                starts.push(pos);
                pos += l as usize;
            }
            starts.push(pos);
            if pos > blob.len() {
                return Err(ints::truncated());
            }
            let mut ii = indices.into_iter();
            emit(&mut data, &mut ends, &mut |out| {
                let k = ii.next().ok_or_else(ints::truncated)? as usize;
                if k >= entries {
                    return Err(ints::truncated());
                }
                out.extend_from_slice(&blob[starts[k]..starts[k + 1]]);
                Ok(())
            })?;
        }
        INT => {
            let values = ints::decode_all(&streams()?)?;
            let mut vi = values.into_iter();
            emit(&mut data, &mut ends, &mut |out| {
                let v = vi.next().ok_or_else(ints::truncated)?;
                let mut buf = itoa::Buffer::new();
                out.extend_from_slice(buf.format(v).as_bytes());
                Ok(())
            })?;
        }
        DECIMAL => {
            let scale = meta.u8()?;
            let scales = if scale == 255 { Some(ints::decode_all(&streams()?)?) } else { None };
            let mantissas = ints::decode_all(&streams()?)?;
            let mut mi = mantissas.into_iter();
            let mut si = scales.map(|s| s.into_iter());
            emit(&mut data, &mut ends, &mut |out| {
                let m = mi.next().ok_or_else(ints::truncated)?;
                let s = match &mut si {
                    Some(it) => it.next().ok_or_else(ints::truncated)? as u8,
                    None => scale,
                };
                write_decimal(out, m, s);
                Ok(())
            })?;
        }
        TEMPLATE => {
            let nparts = meta.usize()?;
            let mut parts = Vec::with_capacity(nparts);
            for _ in 0..nparts {
                match meta.u8()? {
                    0 => {
                        let l = meta.usize()?;
                        parts.push(Part::Literal(meta.bytes(l)?.to_vec()));
                    }
                    1 => parts.push(Part::Field(meta.u8()?)),
                    _ => return Err(ints::truncated()),
                }
            }
            let nfields = parts.iter().filter(|p| matches!(p, Part::Field(_))).count();
            let mut fields = Vec::with_capacity(nfields);
            for _ in 0..nfields {
                fields.push(ints::decode_all(&streams()?)?.into_iter());
            }
            emit(&mut data, &mut ends, &mut |out| {
                let mut f = 0;
                for p in &parts {
                    match p {
                        Part::Literal(l) => out.extend_from_slice(l),
                        Part::Field(w) => {
                            let v = fields[f].next().ok_or_else(ints::truncated)?;
                            write_field(out, v, *w);
                            f += 1;
                        }
                    }
                }
                Ok(())
            })?;
        }
        other => return Err(Error::Format(format!("unknown DCZ column encoding {other}"))),
    }
    Ok(Cells { data, ends, quoted })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(cells: &[&str], expect_enc: Option<u8>) {
        let bytes: Vec<&[u8]> = cells.iter().map(|c| c.as_bytes()).collect();
        let quoted: Vec<bool> = (0..cells.len()).map(|i| i % 5 == 1).collect();
        for exhaustive in [false, true] {
            for col in candidates(&bytes, &quoted, exhaustive) {
                let enc = col.meta[0];
                let mut streams = col.streams.clone().into_iter();
                let mut next = || streams.next().ok_or_else(ints::truncated);
                let mut meta = Cursor::new(&col.meta);
                let decoded = decode_column(&mut meta, &mut next, cells.len()).unwrap();
                for (i, c) in cells.iter().enumerate() {
                    assert_eq!(decoded.get(i), c.as_bytes(), "enc {enc} cell {i}");
                    assert_eq!(decoded.quoted[i], quoted[i]);
                }
            }
        }
        if let Some(e) = expect_enc {
            assert_eq!(candidates(&bytes, &quoted, false)[0].meta[0], e);
        }
    }

    #[test]
    fn encoders_roundtrip() {
        roundtrip(&["1", "22", "", "-3", "0", "4"], Some(INT));
        roundtrip(&["1.50", "2.25", "", "-0.5", "10", "0.05"], Some(DECIMAL));
        roundtrip(&["x", "x", "x"], Some(CONST));
        let ts: Vec<String> =
            (0..300).map(|i| format!("2024-01-{:02} 10:{:02}:{:02}", i % 28 + 1, i % 60, (i * 7) % 60)).collect();
        let ts_ref: Vec<&str> = ts.iter().map(|s| s.as_str()).collect();
        roundtrip(&ts_ref, Some(TEMPLATE));
        let ids: Vec<String> = (0..300).map(|i| format!("INV-{:06}", i * 13)).collect();
        roundtrip(&ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(), Some(TEMPLATE));
        let cats: Vec<&str> = (0..300).map(|i| ["red", "green", "blue"][i % 3]).collect();
        roundtrip(&cats, Some(DICT));
        let words: Vec<String> = (0..300).map(|i| format!("word{i} free text {}", i * i)).collect();
        roundtrip(&words.iter().map(|s| s.as_str()).collect::<Vec<_>>(), None);
        // tricky values become exceptions but still round-trip
        roundtrip(&["1", "2", "007", "-0", "3", "4", "5", "6", "7", "8", "9", "10"], Some(INT));
        roundtrip(&["-0.0", "1.5", "2.5", "3.5", "4.5", "5.5", "6.5", "7.5", "8.5", "9.5"], Some(DECIMAL));
    }

    #[test]
    fn decimals() {
        for (s, m, sc) in [("12.30", 1230, 2), ("0.05", 5, 2), ("-0.5", -5, 1), ("7", 7, 0)] {
            assert_eq!(canonical_decimal(s.as_bytes()), Some((m, sc)));
            let mut out = Vec::new();
            write_decimal(&mut out, m, sc);
            assert_eq!(out, s.as_bytes());
        }
        for bad in ["-0", "-0.00", "00.5", ".5", "5.", "1e5", "+1"] {
            assert_eq!(canonical_decimal(bad.as_bytes()), None, "{bad}");
        }
    }
}
