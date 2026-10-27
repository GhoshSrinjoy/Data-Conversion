//! Integer-array codec: the workhorse behind every numeric DCZ stream.
//!
//! ```text
//! int_array := count:varint transform:u8 body
//!   0 PLAIN   base:zigzag width:u8 planes        values = base + unsigned(width bytes)
//!   1 DELTA   first:zigzag base:zigzag width:u8 planes   deltas of consecutive values
//!   2 RLE     runs:varint values:int_array lengths:int_array
//! ```
//! Fixed-width little-endian values are stored as byte planes (all first bytes, then
//! all second bytes, ...), so the near-constant high bytes form long runs that the
//! entropy coder removes almost for free.  A width of 0 means every value equals the
//! base, which makes constant columns and constant-step sequences cost a few bytes.

use crate::error::{Error, Result};

#[inline]
pub fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[inline]
pub fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

#[inline]
pub fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

pub struct Cursor<'a> {
    pub data: &'a [u8],
    pub pos: usize,
}

impl<'a> Cursor<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Cursor { data, pos: 0 }
    }

    #[inline]
    pub fn u8(&mut self) -> Result<u8> {
        let b = *self.data.get(self.pos).ok_or_else(truncated)?;
        self.pos += 1;
        Ok(b)
    }

    #[inline]
    pub fn varint(&mut self) -> Result<u64> {
        let mut v = 0u64;
        let mut shift = 0;
        loop {
            let b = self.u8()?;
            if shift >= 64 {
                return Err(Error::Integrity("varint overflow".into()));
            }
            v |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(v);
            }
            shift += 7;
        }
    }

    pub fn zigzag(&mut self) -> Result<i64> {
        Ok(unzigzag(self.varint()?))
    }

    pub fn usize(&mut self) -> Result<usize> {
        let v = self.varint()?;
        usize::try_from(v).map_err(|_| Error::Integrity("length out of range".into()))
    }

    pub fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(truncated)?;
        let s = self.data.get(self.pos..end).ok_or_else(truncated)?;
        self.pos = end;
        Ok(s)
    }

    pub fn is_empty(&self) -> bool {
        self.pos >= self.data.len()
    }
}

pub fn truncated() -> Error {
    Error::Integrity("DCZ data is truncated or corrupt".into())
}

#[inline]
fn width_for(max: u64) -> u8 {
    ((64 - max.leading_zeros() + 7) / 8) as u8
}

/// Append `values` as `width`-byte little-endian unsigned numbers in byte planes.
fn put_planes(out: &mut Vec<u8>, values: &[u64], width: u8) {
    let n = values.len();
    let start = out.len();
    out.resize(start + n * width as usize, 0);
    let dst = &mut out[start..];
    for plane in 0..width as usize {
        let shift = plane * 8;
        let row = &mut dst[plane * n..(plane + 1) * n];
        for (slot, &v) in row.iter_mut().zip(values) {
            *slot = (v >> shift) as u8;
        }
    }
}

fn get_planes(c: &mut Cursor<'_>, n: usize, width: u8) -> Result<Vec<u64>> {
    let data = c.bytes(n.checked_mul(width as usize).ok_or_else(truncated)?)?;
    let mut out = vec![0u64; n];
    for plane in 0..width as usize {
        let shift = plane * 8;
        let row = &data[plane * n..(plane + 1) * n];
        for (v, &b) in out.iter_mut().zip(row) {
            *v |= (b as u64) << shift;
        }
    }
    Ok(out)
}

fn put_for(out: &mut Vec<u8>, values: &[i64]) {
    let base = values.iter().copied().min().unwrap_or(0);
    let offsets: Vec<u64> = values.iter().map(|&v| v.wrapping_sub(base) as u64).collect();
    let width = width_for(offsets.iter().copied().max().unwrap_or(0));
    put_varint(out, zigzag(base));
    out.push(width);
    put_planes(out, &offsets, width);
}

fn get_for(c: &mut Cursor<'_>, n: usize) -> Result<Vec<i64>> {
    let base = c.zigzag()?;
    let width = c.u8()?;
    if width > 8 {
        return Err(truncated());
    }
    let offsets = get_planes(c, n, width)?;
    Ok(offsets.into_iter().map(|o| base.wrapping_add(o as i64)).collect())
}

fn deltas(values: &[i64]) -> Vec<i64> {
    values.windows(2).map(|w| w[1].wrapping_sub(w[0])).collect()
}

fn estimate_for(values: &[i64]) -> usize {
    match (values.iter().min(), values.iter().max()) {
        (Some(&lo), Some(&hi)) => values.len() * width_for(hi.wrapping_sub(lo) as u64) as usize,
        _ => 0,
    }
}

fn run_count(values: &[i64]) -> usize {
    if values.is_empty() {
        return 0;
    }
    1 + values.windows(2).filter(|w| w[0] != w[1]).count()
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Transform {
    Plain = 0,
    Delta = 1,
    Rle = 2,
}

/// Pick the transform with the smallest estimated size.
pub fn choose(values: &[i64]) -> Transform {
    if values.len() < 4 {
        return Transform::Plain;
    }
    let plain = estimate_for(values);
    let d = deltas(values);
    let delta = estimate_for(&d) + 9;
    let runs = run_count(values);
    let rle = if runs * 4 <= values.len() { runs * 3 + 16 } else { usize::MAX };
    if rle < plain.min(delta) {
        Transform::Rle
    } else if delta < plain {
        Transform::Delta
    } else {
        Transform::Plain
    }
}

pub fn encode_with(out: &mut Vec<u8>, values: &[i64], t: Transform) {
    put_varint(out, values.len() as u64);
    out.push(t as u8);
    if values.is_empty() {
        return;
    }
    match t {
        Transform::Plain => put_for(out, values),
        Transform::Delta => {
            put_varint(out, zigzag(values[0]));
            put_for(out, &deltas(values));
        }
        Transform::Rle => {
            let mut run_values = Vec::new();
            let mut run_lengths = Vec::new();
            let mut i = 0;
            while i < values.len() {
                let v = values[i];
                let mut j = i + 1;
                while j < values.len() && values[j] == v {
                    j += 1;
                }
                run_values.push(v);
                run_lengths.push((j - i) as i64);
                i = j;
            }
            put_varint(out, run_values.len() as u64);
            let tv = choose(&run_values);
            encode_with(out, &run_values, if tv == Transform::Rle { Transform::Plain } else { tv });
            encode_with(out, &run_lengths, Transform::Plain);
        }
    }
}

pub fn encode(out: &mut Vec<u8>, values: &[i64]) {
    encode_with(out, values, choose(values));
}

pub fn decode(c: &mut Cursor<'_>) -> Result<Vec<i64>> {
    let n = c.usize()?;
    let t = c.u8()?;
    if n == 0 {
        return Ok(Vec::new());
    }
    if n > (1 << 34) {
        return Err(truncated());
    }
    match t {
        0 => get_for(c, n),
        1 => {
            let first = c.zigzag()?;
            let d = get_for(c, n - 1)?;
            let mut out = Vec::with_capacity(n);
            out.push(first);
            let mut acc = first;
            for x in d {
                acc = acc.wrapping_add(x);
                out.push(acc);
            }
            Ok(out)
        }
        2 => {
            let runs = c.usize()?;
            let values = decode(c)?;
            let lengths = decode(c)?;
            if values.len() != runs || lengths.len() != runs {
                return Err(truncated());
            }
            let mut out = Vec::with_capacity(n);
            for (v, len) in values.into_iter().zip(lengths) {
                if len < 0 || out.len() + len as usize > n {
                    return Err(truncated());
                }
                out.extend(std::iter::repeat(v).take(len as usize));
            }
            if out.len() != n {
                return Err(truncated());
            }
            Ok(out)
        }
        _ => Err(Error::Format(format!("unknown integer transform {t}"))),
    }
}

pub fn encode_to_vec(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 2 + 16);
    encode(&mut out, values);
    out
}

pub fn decode_all(data: &[u8]) -> Result<Vec<i64>> {
    let mut c = Cursor::new(data);
    decode(&mut c)
}

/// Pack booleans LSB-first.
pub fn put_bitmap(out: &mut Vec<u8>, bits: &[bool]) {
    let start = out.len();
    out.resize(start + bits.len().div_ceil(8), 0);
    for (i, &b) in bits.iter().enumerate() {
        if b {
            out[start + i / 8] |= 1 << (i % 8);
        }
    }
}

pub fn get_bitmap(c: &mut Cursor<'_>, n: usize) -> Result<Vec<bool>> {
    let data = c.bytes(n.div_ceil(8))?;
    Ok((0..n).map(|i| data[i / 8] & (1 << (i % 8)) != 0).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(values: &[i64]) -> usize {
        for t in [Transform::Plain, Transform::Delta, Transform::Rle] {
            let mut out = Vec::new();
            encode_with(&mut out, values, t);
            assert_eq!(decode_all(&out).unwrap(), values, "{t:?}");
        }
        let out = encode_to_vec(values);
        assert_eq!(decode_all(&out).unwrap(), values);
        out.len()
    }

    #[test]
    fn varints() {
        for v in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut out = Vec::new();
            put_varint(&mut out, v);
            assert_eq!(Cursor::new(&out).varint().unwrap(), v);
        }
        for v in [0i64, -1, 1, i64::MIN, i64::MAX, -12345] {
            assert_eq!(unzigzag(zigzag(v)), v);
        }
    }

    #[test]
    fn arrays() {
        roundtrip(&[]);
        roundtrip(&[5]);
        roundtrip(&[i64::MIN, i64::MAX, 0, -1, 1]);
        let seq: Vec<i64> = (0..10_000).collect();
        assert!(roundtrip(&seq) < 32, "constant step must be nearly free");
        let ts: Vec<i64> = (0..10_000).map(|i| 1_700_000_000 + i * 60).collect();
        assert!(roundtrip(&ts) < 40);
        let runs: Vec<i64> = (0..10_000).map(|i| i / 1000).collect();
        assert!(roundtrip(&runs) < 64);
        let mut x = 12345u64;
        let random: Vec<i64> = (0..10_000)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x % 1000) as i64 - 500
            })
            .collect();
        let size = roundtrip(&random);
        assert!(size <= 10_000 * 2 + 32);
    }

    #[test]
    fn corrupt_input_errors() {
        let out = encode_to_vec(&(0..100).map(|i| i * 3 + 7).collect::<Vec<i64>>());
        for cut in 0..out.len() {
            let _ = decode_all(&out[..cut]); // must not panic
        }
        assert!(decode_all(&[5, 9]).is_err());
    }

    #[test]
    fn bitmaps() {
        let bits: Vec<bool> = (0..37).map(|i| i % 3 == 0).collect();
        let mut out = Vec::new();
        put_bitmap(&mut out, &bits);
        assert_eq!(get_bitmap(&mut Cursor::new(&out), 37).unwrap(), bits);
    }
}
