//! CSV output with exact, lossless formatting.
//!
//! * a null is written as an empty unquoted field and an empty string as `""`, so the
//!   two stay distinguishable when the file is read back;
//! * floats use the shortest representation that parses back to the same double;
//! * timestamps are ISO 8601 with the column's own UTC offset;
//! * rows are formatted in parallel and written in order.

use std::io::Write;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float16Array, Float32Array,
    Float64Array, GenericStringArray, Int16Array, Int32Array, Int64Array, Int8Array, OffsetSizeTrait, RecordBatch,
    StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, FieldRef, TimeUnit};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use rayon::prelude::*;

use crate::error::Result;

#[derive(Clone, Debug)]
pub struct CsvWriteOptions {
    pub delimiter: u8,
    pub quote: u8,
    pub terminator: Vec<u8>,
    pub header: bool,
}

impl Default for CsvWriteOptions {
    fn default() -> Self {
        CsvWriteOptions { delimiter: b',', quote: b'"', terminator: b"\n".to_vec(), header: true }
    }
}

#[inline]
fn needs_quotes(v: &[u8], o: &CsvWriteOptions) -> bool {
    v.is_empty()
        || v[0] == b' '
        || v[v.len() - 1] == b' '
        || memchr::memchr3(o.delimiter, o.quote, b'\n', v).is_some()
        || memchr::memchr(b'\r', v).is_some()
}

#[inline]
pub fn write_text(out: &mut Vec<u8>, v: &[u8], o: &CsvWriteOptions) {
    if needs_quotes(v, o) {
        out.reserve(v.len() + 2);
        out.push(o.quote);
        // a quote inside the value is written twice
        let mut rest = v;
        while let Some(k) = memchr::memchr(o.quote, rest) {
            out.extend_from_slice(&rest[..=k]);
            out.push(o.quote);
            rest = &rest[k + 1..];
        }
        out.extend_from_slice(rest);
        out.push(o.quote);
    } else {
        out.extend_from_slice(v);
    }
}

pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

pub(crate) fn write_date(out: &mut Vec<u8>, days: i64) {
    let (y, m, d) = civil_from_days(days);
    if (0..=9999).contains(&y) {
        let _ = write!(out, "{y:04}-{m:02}-{d:02}");
    } else {
        let _ = write!(out, "{y}-{m:02}-{d:02}");
    }
}

/// A date64 (milliseconds) as a date.  A value that is not a whole day, which the
/// format forbids but files can hold, keeps its time as a timestamp.
pub(crate) fn write_date64(out: &mut Vec<u8>, ms: i64) {
    if ms.rem_euclid(86_400_000) == 0 {
        write_date(out, ms.div_euclid(86_400_000));
    } else {
        write_timestamp(out, ms, TimeUnit::Millisecond, None);
    }
}

fn tz_offset_minutes(tz: &str) -> Option<i32> {
    if tz.eq_ignore_ascii_case("utc") || tz == "Z" || tz == "+00:00" {
        return Some(0);
    }
    let b = tz.as_bytes();
    if b.len() == 6 && (b[0] == b'+' || b[0] == b'-') && b[3] == b':' {
        let h: i32 = tz[1..3].parse().ok()?;
        let m: i32 = tz[4..6].parse().ok()?;
        return Some(if b[0] == b'-' { -(h * 60 + m) } else { h * 60 + m });
    }
    None
}

fn write_timestamp(out: &mut Vec<u8>, value: i64, unit: TimeUnit, offset: Option<i32>) {
    let per_sec: i64 = match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    };
    let mut secs = value.div_euclid(per_sec);
    let frac = value.rem_euclid(per_sec);
    if let Some(m) = offset {
        secs += m as i64 * 60;
    }
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    write_date(out, days);
    let _ = write!(out, "T{:02}:{:02}:{:02}", tod / 3600, tod / 60 % 60, tod % 60);
    if frac != 0 {
        let width = match unit {
            TimeUnit::Second => 0,
            TimeUnit::Millisecond => 3,
            TimeUnit::Microsecond => 6,
            TimeUnit::Nanosecond => 9,
        };
        let digits = format!("{frac:0width$}");
        out.push(b'.');
        out.extend_from_slice(digits.trim_end_matches('0').as_bytes());
    }
    match offset {
        Some(0) => out.push(b'Z'),
        Some(m) => {
            let sign = if m < 0 { '-' } else { '+' };
            let _ = write!(out, "{sign}{:02}:{:02}", m.abs() / 60, m.abs() % 60);
        }
        None => {}
    }
}

/// The UTC offset a timestamp column is printed in: its own fixed offset, UTC for
/// named zones (whose rules are not at hand), none without a time zone.
pub(crate) fn timestamp_offset(tz: Option<&str>) -> Option<i32> {
    tz.map(|tz| tz_offset_minutes(tz).unwrap_or(0))
}

/// A timestamp column's value at `i`, as CSV and JSON output write it.
pub(crate) fn write_timestamp_at(out: &mut Vec<u8>, a: &dyn Array, i: usize, unit: TimeUnit, offset: Option<i32>) {
    let v = match unit {
        TimeUnit::Second => a.as_any().downcast_ref::<TimestampSecondArray>().unwrap().value(i),
        TimeUnit::Millisecond => a.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap().value(i),
        TimeUnit::Microsecond => a.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(i),
        TimeUnit::Nanosecond => a.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value(i),
    };
    write_timestamp(out, v, unit, offset);
}

pub fn write_decimal(out: &mut Vec<u8>, v: i128, scale: i8) {
    if scale <= 0 {
        let _ = write!(out, "{v}");
        for _ in 0..(-scale) {
            out.push(b'0');
        }
        return;
    }
    let s = scale as u32;
    let neg = v < 0;
    let abs = v.unsigned_abs();
    let pow = 10u128.pow(s);
    if neg {
        out.push(b'-');
    }
    let _ = write!(out, "{}.{:0width$}", abs / pow, abs % pow, width = s as usize);
}

/// Formats one column's value at a row into an output buffer.
enum Col<'a> {
    Str(&'a dyn StrLike),
    Bool(&'a BooleanArray),
    I8(&'a Int8Array),
    I16(&'a Int16Array),
    I32(&'a Int32Array),
    I64(&'a Int64Array),
    U8(&'a UInt8Array),
    U16(&'a UInt16Array),
    U32(&'a UInt32Array),
    U64(&'a UInt64Array),
    /// float16 is written as the float32 it widens to.
    F16(&'a Float16Array),
    F32(&'a Float32Array),
    F64(&'a Float64Array),
    Dec(&'a Decimal128Array, i8),
    Date(&'a Date32Array),
    Date64(&'a Date64Array),
    Ts(&'a dyn Array, TimeUnit, Option<i32>),
    /// Structs, lists and maps as JSON text.
    Json(arrow::json::writer::NullableEncoder<'a>),
    Other(ArrayFormatter<'a>),
}

trait StrLike: Sync {
    fn str_value(&self, i: usize) -> &str;
}

impl<O: OffsetSizeTrait> StrLike for GenericStringArray<O> {
    fn str_value(&self, i: usize) -> &str {
        self.value(i)
    }
}

impl StrLike for StringViewArray {
    fn str_value(&self, i: usize) -> &str {
        self.value(i)
    }
}

macro_rules! int {
    ($out:expr, $a:expr, $i:expr) => {{
        let mut buf = itoa::Buffer::new();
        $out.extend_from_slice(buf.format($a.value($i)).as_bytes());
    }};
}

fn float_text(out: &mut Vec<u8>, v: f64) {
    if v.is_nan() {
        out.extend_from_slice(b"NaN");
    } else if v.is_infinite() {
        out.extend_from_slice(if v > 0.0 { b"inf" } else { b"-inf" });
    } else {
        let mut buf = ryu::Buffer::new();
        let s = buf.format_finite(v);
        // ryu prints whole numbers as "3.0": keep it, it re-reads as a float.
        out.extend_from_slice(s.as_bytes());
    }
}

fn single_text(out: &mut Vec<u8>, v: f32) {
    if v.is_finite() {
        let mut buf = ryu::Buffer::new();
        out.extend_from_slice(buf.format_finite(v).as_bytes());
    } else {
        float_text(out, v as f64);
    }
}

impl<'a> Col<'a> {
    fn new(
        field: &'a FieldRef,
        array: &'a ArrayRef,
        fmt: &'a FormatOptions<'a>,
        json: &'a arrow::json::writer::EncoderOptions,
    ) -> Result<Col<'a>> {
        Ok(match array.data_type() {
            DataType::Utf8 => Col::Str(array.as_string::<i32>()),
            DataType::LargeUtf8 => Col::Str(array.as_string::<i64>()),
            DataType::Utf8View => Col::Str(array.as_string_view()),
            DataType::Boolean => Col::Bool(array.as_boolean()),
            DataType::Int8 => Col::I8(array.as_primitive()),
            DataType::Int16 => Col::I16(array.as_primitive()),
            DataType::Int32 => Col::I32(array.as_primitive()),
            DataType::Int64 => Col::I64(array.as_primitive()),
            DataType::UInt8 => Col::U8(array.as_primitive()),
            DataType::UInt16 => Col::U16(array.as_primitive()),
            DataType::UInt32 => Col::U32(array.as_primitive()),
            DataType::UInt64 => Col::U64(array.as_primitive()),
            DataType::Float16 => Col::F16(array.as_primitive()),
            DataType::Float32 => Col::F32(array.as_primitive()),
            DataType::Float64 => Col::F64(array.as_primitive()),
            DataType::Decimal128(_, s) => Col::Dec(array.as_primitive(), *s),
            DataType::Date32 => Col::Date(array.as_primitive()),
            DataType::Date64 => Col::Date64(array.as_primitive()),
            DataType::Timestamp(unit, tz) => Col::Ts(array.as_ref(), *unit, timestamp_offset(tz.as_deref())),
            DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(..)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::Map(..) => Col::Json(arrow::json::writer::make_encoder(field, array.as_ref(), json)?),
            _ => Col::Other(ArrayFormatter::try_new(array.as_ref(), fmt)?),
        })
    }

    fn write(&mut self, out: &mut Vec<u8>, i: usize, o: &CsvWriteOptions) {
        match self {
            Col::Str(a) => write_text(out, a.str_value(i).as_bytes(), o),
            Col::Bool(a) => out.extend_from_slice(if a.value(i) { b"true" } else { b"false" }),
            Col::I8(a) => int!(out, a, i),
            Col::I16(a) => int!(out, a, i),
            Col::I32(a) => int!(out, a, i),
            Col::I64(a) => int!(out, a, i),
            Col::U8(a) => int!(out, a, i),
            Col::U16(a) => int!(out, a, i),
            Col::U32(a) => int!(out, a, i),
            Col::U64(a) => int!(out, a, i),
            Col::F16(a) => single_text(out, a.value(i).to_f32()),
            Col::F32(a) => single_text(out, a.value(i)),
            Col::F64(a) => float_text(out, a.value(i)),
            Col::Dec(a, s) => write_decimal(out, a.value(i), *s),
            Col::Date(a) => write_date(out, a.value(i) as i64),
            Col::Date64(a) => write_date64(out, a.value(i)),
            Col::Ts(a, unit, offset) => write_timestamp_at(out, *a, i, *unit, *offset),
            Col::Json(e) => {
                let mut text = Vec::new();
                e.encode(i, &mut text);
                write_text(out, &text, o);
            }
            Col::Other(f) => {
                let text = f.value(i).to_string();
                write_text(out, text.as_bytes(), o);
            }
        }
    }
}

pub fn write_header<W: Write>(w: &mut W, names: &[String], o: &CsvWriteOptions) -> Result<()> {
    let mut out = Vec::new();
    for (i, n) in names.iter().enumerate() {
        if i > 0 {
            out.push(o.delimiter);
        }
        if n.is_empty() {
            out.push(o.quote);
            out.push(o.quote);
        } else {
            write_text(&mut out, n.as_bytes(), o);
        }
    }
    out.extend_from_slice(&o.terminator);
    w.write_all(&out)?;
    Ok(())
}

fn format_rows(batch: &RecordBatch, start: usize, end: usize, o: &CsvWriteOptions) -> Result<Vec<u8>> {
    let fmt = FormatOptions::default().with_null("");
    let json = arrow::json::writer::EncoderOptions::default()
        .with_explicit_nulls(true)
        .with_encoder_factory(std::sync::Arc::new(crate::jsonl::FloatText));
    let schema = batch.schema();
    let mut cols: Vec<Col> =
        schema.fields().iter().zip(batch.columns()).map(|(f, c)| Col::new(f, c, &fmt, &json)).collect::<Result<_>>()?;
    // logical nulls: a null-type column, or a dictionary whose values hold nulls, has
    // no validity bits of its own
    let nulls: Vec<_> = batch.columns().iter().map(|a| a.logical_nulls()).collect();
    let mut out = Vec::with_capacity((end - start) * 16 * cols.len().max(1));
    for row in start..end {
        for (j, col) in cols.iter_mut().enumerate() {
            if j > 0 {
                out.push(o.delimiter);
            }
            if !nulls[j].as_ref().is_some_and(|n| n.is_null(row)) {
                col.write(&mut out, row, o);
            }
        }
        out.extend_from_slice(&o.terminator);
    }
    Ok(out)
}

/// Format a batch (in parallel slices) and write it.
pub fn write_batch<W: Write>(w: &mut W, batch: &RecordBatch, o: &CsvWriteOptions) -> Result<()> {
    let rows = batch.num_rows();
    const SLICE: usize = 16_384;
    if rows <= SLICE {
        w.write_all(&format_rows(batch, 0, rows, o)?)?;
        return Ok(());
    }
    let ranges: Vec<(usize, usize)> = (0..rows).step_by(SLICE).map(|s| (s, (s + SLICE).min(rows))).collect();
    let parts: Vec<Vec<u8>> = ranges.par_iter().map(|&(s, e)| format_rows(batch, s, e, o)).collect::<Result<_>>()?;
    for p in parts {
        w.write_all(&p)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn formats_types_losslessly() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("f", DataType::Float64, true),
            Field::new("d", DataType::Decimal128(10, 2), true),
            Field::new("t", DataType::Timestamp(TimeUnit::Millisecond, Some("+05:30".into())), true),
            Field::new("day", DataType::Date32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a,b"), Some(""), None, Some("x \"q\"")])),
                Arc::new(Float64Array::from(vec![Some(0.1), Some(1e20), None, Some(f64::NAN)])),
                Arc::new(
                    Decimal128Array::from(vec![Some(12345), Some(-5), None, Some(0)])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
                Arc::new(
                    TimestampMillisecondArray::from(vec![Some(0), Some(1500), None, Some(-1)]).with_timezone("+05:30"),
                ),
                Arc::new(Date32Array::from(vec![Some(0), Some(19723), None, Some(-1)])),
            ],
        )
        .unwrap();
        let mut out = Vec::new();
        write_batch(&mut out, &batch, &CsvWriteOptions::default()).unwrap();
        let text = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], "\"a,b\",0.1,123.45,1970-01-01T05:30:00+05:30,1970-01-01");
        assert_eq!(lines[1], "\"\",1e20,-0.05,1970-01-01T05:30:01.5+05:30,2024-01-01");
        assert_eq!(lines[2], ",,,,");
        assert_eq!(lines[3], "\"x \"\"q\"\"\",NaN,0.00,1970-01-01T05:29:59.999+05:30,1969-12-31");
    }

    #[test]
    fn civil_roundtrip() {
        for days in [-800_000i64, -1, 0, 1, 11017, 19723, 2_932_896] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(crate::types::days_from_civil(y, m, d), days);
        }
    }
}
