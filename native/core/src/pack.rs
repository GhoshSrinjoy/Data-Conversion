//! Lossless packing of in-memory value lists (ints, floats, strings, booleans).
//!
//! The result is a compact DCZ frame (header flag `FLAG_PACKED`) of kind 2:
//! ```text
//! payload := streams (as in block.rs); stream 0 (meta) :=
//!   type:u8 count:varint has_nulls:u8 tag_len:varint tag[tag_len] encoding params...
//!   type 1 = int64, 2 = float64, 3 = utf-8 strings, 4 = bool
//!   [nulls bitmap stream] then type-specific streams over the non-null values:
//!   int64 : values:int_array
//!   float64 enc 0 (decimal): exponent:u8 exceptions:varint
//!                  streams mantissas:int_array, [positions:int_array, bits:u64le*]
//!           enc 1 (xor planes): streams 8 byte planes of bits ^ previous bits
//!   strings: a text column (see text.rs)
//!   bool   : bitmap stream
//! ```
//! Floats are bit-exact: a value decodes as `mantissa / 10^exponent`, which is only
//! used when that division reproduces the original bits; anything else (NaN payloads,
//! -0.0, values needing more digits) is stored verbatim as an exception.

use super::dcz::block::stream_codecs;
use crate::codec::{self, CodecId, Profile};
use crate::dcz::ints::{self, put_varint, Cursor};
use crate::dcz::text;
use crate::error::{Error, Result};

pub const KIND_PACK: u8 = 2;

#[derive(Clone, Debug, PartialEq)]
pub enum Values {
    Int(Vec<i64>),
    Float(Vec<f64>),
    Str(Vec<Vec<u8>>),
    Bool(Vec<bool>),
}

impl Values {
    pub fn len(&self) -> usize {
        match self {
            Values::Int(v) => v.len(),
            Values::Float(v) => v.len(),
            Values::Str(v) => v.len(),
            Values::Bool(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Packed {
    pub values: Values,
    /// Validity per position (true = present); None when nothing is missing.
    pub valid: Option<Vec<bool>>,
    /// Opaque caller metadata (e.g. the original dtype).
    pub tag: Vec<u8>,
}

const POW10: [f64; 23] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
    1e20, 1e21, 1e22,
];
const MAX_EXACT: f64 = 9_007_199_254_740_992.0; // 2^53

#[inline]
fn decimal_mantissa(v: f64, e: usize) -> Option<i64> {
    if !v.is_finite() || (v == 0.0 && v.is_sign_negative()) {
        return None;
    }
    let scaled = v * POW10[e];
    if scaled.abs() >= MAX_EXACT {
        return None;
    }
    let m = scaled.round();
    if (m / POW10[e]).to_bits() == v.to_bits() {
        Some(m as i64)
    } else {
        None
    }
}

fn best_exponent(values: &[f64]) -> (usize, usize) {
    let step = (values.len() / 2048).max(1);
    let sample: Vec<f64> = values.iter().step_by(step).copied().collect();
    let mut best = (0usize, usize::MAX);
    for e in 0..=18 {
        let misses = sample.iter().filter(|&&v| decimal_mantissa(v, e).is_none()).count();
        if misses < best.1 {
            best = (e, misses);
            if misses == 0 {
                break;
            }
        }
    }
    best
}

fn stream(out: &mut Vec<(CodecId, usize, Vec<u8>)>, data: Vec<u8>, profile: Profile) -> Result<()> {
    let (c, bytes) = if data.len() < 48 { (CodecId::None, data.clone()) } else { codec::compress_best(&stream_codecs(profile), &data)? };
    out.push((c, data.len(), bytes));
    Ok(())
}

fn float_streams(values: &[f64], profile: Profile) -> Result<(Vec<u8>, Vec<(CodecId, usize, Vec<u8>)>)> {
    let mut candidates: Vec<(Vec<u8>, Vec<(CodecId, usize, Vec<u8>)>)> = Vec::new();
    let (e, misses) = best_exponent(values);
    if misses * 4 < values.len().max(1) {
        let mut mantissas = Vec::with_capacity(values.len());
        let mut positions = Vec::new();
        let mut bits = Vec::new();
        let mut last = 0i64;
        for (i, &v) in values.iter().enumerate() {
            match decimal_mantissa(v, e) {
                Some(m) => {
                    mantissas.push(m);
                    last = m;
                }
                None => {
                    mantissas.push(last);
                    positions.push(i as i64);
                    bits.extend_from_slice(&v.to_bits().to_le_bytes());
                }
            }
        }
        let mut params = vec![0u8, e as u8];
        put_varint(&mut params, positions.len() as u64);
        let mut s = Vec::new();
        stream(&mut s, ints::encode_to_vec(&mantissas), profile)?;
        if !positions.is_empty() {
            let mut exc = ints::encode_to_vec(&positions);
            exc.extend_from_slice(&bits);
            stream(&mut s, exc, profile)?;
        }
        candidates.push((params, s));
    }
    {
        let n = values.len();
        let mut planes = vec![0u8; n * 8];
        let mut prev = 0u64;
        for (i, &v) in values.iter().enumerate() {
            let x = v.to_bits() ^ prev;
            prev = v.to_bits();
            for b in 0..8 {
                planes[b * n + i] = (x >> (8 * b)) as u8;
            }
        }
        let mut s = Vec::new();
        stream(&mut s, planes, profile)?;
        candidates.push((vec![1u8], s));
    }
    Ok(candidates
        .into_iter()
        .min_by_key(|(p, s)| p.len() + s.iter().map(|x| x.2.len() + 6).sum::<usize>())
        .expect("at least one float encoding"))
}

pub fn pack(p: &Packed, profile: Profile) -> Result<Vec<u8>> {
    let count = p.valid.as_ref().map_or(p.values.len(), |v| v.len());
    if let Some(valid) = &p.valid {
        let present = valid.iter().filter(|&&x| x).count();
        if present != p.values.len() {
            return Err(Error::Invalid("validity mask does not match the number of values".into()));
        }
    }
    let ty = match &p.values {
        Values::Int(_) => 1u8,
        Values::Float(_) => 2,
        Values::Str(_) => 3,
        Values::Bool(_) => 4,
    };
    let mut meta = vec![ty];
    put_varint(&mut meta, count as u64);
    meta.push(p.valid.is_some() as u8);
    put_varint(&mut meta, p.tag.len() as u64);
    meta.extend_from_slice(&p.tag);
    let mut streams = Vec::new();
    if let Some(valid) = &p.valid {
        let mut bm = Vec::new();
        ints::put_bitmap(&mut bm, valid);
        stream(&mut streams, bm, profile)?;
    }
    match &p.values {
        Values::Int(v) => stream(&mut streams, ints::encode_to_vec(v), profile)?,
        Values::Bool(v) => {
            let mut bm = Vec::new();
            ints::put_bitmap(&mut bm, v);
            stream(&mut streams, bm, profile)?;
        }
        Values::Float(v) => {
            let (params, s) = float_streams(v, profile)?;
            meta.extend_from_slice(&params);
            streams.extend(s);
        }
        Values::Str(v) => {
            let cells: Vec<&[u8]> = v.iter().map(|s| s.as_slice()).collect();
            let quoted = vec![false; cells.len()];
            let exhaustive = profile == Profile::Max;
            let mut best: Option<(usize, Vec<u8>, Vec<(CodecId, usize, Vec<u8>)>)> = None;
            for cand in text::candidates(&cells, &quoted, exhaustive) {
                let mut packed = Vec::new();
                for s in cand.streams {
                    stream(&mut packed, s, profile)?;
                }
                let size = cand.meta.len() + packed.iter().map(|x| x.2.len() + 6).sum::<usize>();
                if best.as_ref().map_or(true, |(b, _, _)| size < *b) {
                    best = Some((size, cand.meta, packed));
                }
            }
            let (_, m, s) = best.expect("raw always applies");
            meta.extend_from_slice(&m);
            streams.extend(s);
        }
    }
    let mut all = Vec::with_capacity(streams.len() + 1);
    let (mc, mb) = (CodecId::None, meta.clone());
    all.push((mc, meta.len(), mb));
    all.extend(streams);
    let mut payload = Vec::new();
    put_varint(&mut payload, all.len() as u64);
    for (c, raw, bytes) in &all {
        payload.push(*c as u8);
        put_varint(&mut payload, *raw as u64);
        put_varint(&mut payload, bytes.len() as u64);
    }
    for (_, _, bytes) in &all {
        payload.extend_from_slice(bytes);
    }
    let packed = crate::dcz::wrap_single_block(KIND_PACK, &payload, &meta)?;
    // Verify before handing out the bytes.
    if !same(&unpack(&packed)?, p) {
        return Err(Error::Integrity("pack verification failed".into()));
    }
    Ok(packed)
}

/// Equality with floats compared bit for bit (NaN payloads and -0.0 included).
pub fn same(a: &Packed, b: &Packed) -> bool {
    let values = match (&a.values, &b.values) {
        (Values::Float(x), Values::Float(y)) => x.len() == y.len() && x.iter().zip(y).all(|(p, q)| p.to_bits() == q.to_bits()),
        (x, y) => x == y,
    };
    values && a.valid == b.valid && a.tag == b.tag
}

pub fn unpack(buf: &[u8]) -> Result<Packed> {
    let payload = crate::dcz::single_block_payload(buf, KIND_PACK)?;
    let mut c = Cursor::new(&payload);
    let n = c.usize()?;
    if n > payload.len() {
        return Err(ints::truncated());
    }
    let mut dir = Vec::with_capacity(n);
    for _ in 0..n {
        dir.push((CodecId::from_u8(c.u8()?)?, c.usize()?, c.usize()?));
    }
    let mut pos = c.pos;
    let mut parts = Vec::with_capacity(n);
    for (codec, raw, stored) in dir {
        let bytes = payload.get(pos..pos + stored).ok_or_else(ints::truncated)?;
        pos += stored;
        parts.push(codec::decompress(codec, bytes, raw)?);
    }
    let mut it = parts.into_iter();
    let meta_bytes = it.next().ok_or_else(ints::truncated)?;
    let mut take = move || it.next().ok_or_else(ints::truncated);
    let mut m = Cursor::new(&meta_bytes);
    let ty = m.u8()?;
    let count = m.usize()?;
    let has_nulls = m.u8()? != 0;
    let tag_len = m.usize()?;
    let tag = m.bytes(tag_len)?.to_vec();
    let valid = if has_nulls { Some(ints::get_bitmap(&mut Cursor::new(&take()?), count)?) } else { None };
    let present = valid.as_ref().map_or(count, |v| v.iter().filter(|&&x| x).count());
    let values = match ty {
        1 => Values::Int(ints::decode_all(&take()?)?),
        4 => Values::Bool(ints::get_bitmap(&mut Cursor::new(&take()?), present)?),
        2 => match m.u8()? {
            0 => {
                let e = m.u8()? as usize;
                let n_exc = m.usize()?;
                if e > 22 {
                    return Err(ints::truncated());
                }
                let mantissas = ints::decode_all(&take()?)?;
                let mut out: Vec<f64> = mantissas.iter().map(|&x| x as f64 / POW10[e]).collect();
                if n_exc > 0 {
                    let exc = take()?;
                    let mut ec = Cursor::new(&exc);
                    let positions = ints::decode(&mut ec)?;
                    for p in positions {
                        let b = ec.bytes(8)?;
                        let slot = out.get_mut(p as usize).ok_or_else(ints::truncated)?;
                        *slot = f64::from_bits(u64::from_le_bytes(b.try_into().unwrap()));
                    }
                }
                Values::Float(out)
            }
            1 => {
                let planes = take()?;
                if planes.len() != present * 8 {
                    return Err(ints::truncated());
                }
                let mut out = Vec::with_capacity(present);
                let mut prev = 0u64;
                for i in 0..present {
                    let mut x = 0u64;
                    for b in 0..8 {
                        x |= (planes[b * present + i] as u64) << (8 * b);
                    }
                    prev ^= x;
                    out.push(f64::from_bits(prev));
                }
                Values::Float(out)
            }
            other => return Err(Error::Format(format!("unknown float encoding {other}"))),
        },
        3 => {
            let cells = text::decode_column(&mut m, &mut take, present)?;
            Values::Str((0..present).map(|i| cells.get(i).to_vec()).collect())
        }
        other => return Err(Error::Format(format!("unknown packed type {other}"))),
    };
    if values.len() != present {
        return Err(ints::truncated());
    }
    Ok(Packed { values, valid, tag })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt(values: Values, valid: Option<Vec<bool>>) -> usize {
        let p = Packed { values, valid, tag: b"t".to_vec() };
        let mut size = 0;
        for profile in [Profile::Fast, Profile::Balanced, Profile::Max] {
            let buf = pack(&p, profile).unwrap();
            let back = unpack(&buf).unwrap();
            assert_eq!(back.valid, p.valid);
            match (&back.values, &p.values) {
                (Values::Float(a), Values::Float(b)) => {
                    assert!(a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits()));
                }
                (a, b) => assert_eq!(a, b),
            }
            size = buf.len();
        }
        size
    }

    #[test]
    fn floats_are_bit_exact() {
        let prices: Vec<f64> = (0..100_000).map(|i| ((i * 37) % 100_000) as f64 / 100.0).collect();
        let size = rt(Values::Float(prices), None);
        assert!(size < 100_000 * 3, "decimal floats should pack well, got {size}");
        let specials = vec![0.0, -0.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, f64::MIN_POSITIVE, 1e300, 0.1, 1.0 / 3.0];
        rt(Values::Float(specials), None);
        let noisy: Vec<f64> = (0..10_000).map(|i| (i as f64).sin()).collect();
        rt(Values::Float(noisy), None);
        rt(Values::Float(vec![]), None);
    }

    #[test]
    fn other_types() {
        let seq: Vec<i64> = (0..100_000).map(|i| 1_000_000 + i * 5).collect();
        assert!(rt(Values::Int(seq), None) < 200);
        rt(Values::Int(vec![i64::MIN, 0, i64::MAX]), None);
        rt(Values::Bool(vec![true, false, true]), Some(vec![true, false, true, true]));
        let words: Vec<Vec<u8>> = (0..5000).map(|i| format!("item-{:05}", i % 700).into_bytes()).collect();
        rt(Values::Str(words), None);
        rt(Values::Str(vec![b"".to_vec(), "ünïcode".as_bytes().to_vec()]), None);
    }
}
