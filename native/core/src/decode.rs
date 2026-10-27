//! Guarded column builders: text values in, typed Arrow arrays out.
//!
//! Every push re-checks the losslessness rules of [`crate::types`].  A value that
//! does not fit the planned type is reported as a [`Violation`] (never coerced), and
//! the conversion then re-plans the schema.

use std::sync::Arc;

use arrow::array::builder::{BooleanBufferBuilder, NullBufferBuilder};
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};

use crate::types::{
    is_nan_token, is_null_token, parse_bool, parse_date, parse_decimal, parse_float, parse_int, parse_timestamp,
    IntParse, LogicalType, TypesMode, Tz, Unit,
};

#[derive(Clone, Debug)]
pub struct Violation {
    pub column: usize,
    pub row: usize,
    pub value: String,
}

enum Values {
    Str { offsets: Vec<i32>, data: Vec<u8> },
    Bool(BooleanBufferBuilder),
    Int(Vec<i64>),
    Float(Vec<f64>),
    Decimal { values: Vec<i128>, precision: u8, scale: u8, limit: i128 },
    Date(Vec<i32>),
    Ts { values: Vec<i64>, unit: Unit, tz: Option<String>, expect: Option<Tz> },
}

pub struct ColumnBuilder {
    values: Values,
    nulls: NullBufferBuilder,
    len: usize,
    loose: bool,
}

fn tz_from_name(name: &str) -> Option<Tz> {
    if name.eq_ignore_ascii_case("utc") || name == "Z" || name == "+00:00" {
        return Some(Tz::Offset(0));
    }
    let b = name.as_bytes();
    if b.len() == 6 && (b[0] == b'+' || b[0] == b'-') && b[3] == b':' {
        let h: i32 = name[1..3].parse().ok()?;
        let m: i32 = name[4..6].parse().ok()?;
        let sign = if b[0] == b'-' { -1 } else { 1 };
        return Some(Tz::Offset(sign * (h * 60 + m)));
    }
    None
}

impl ColumnBuilder {
    pub fn new(ty: &LogicalType, capacity: usize, mode: TypesMode) -> Self {
        let values = match ty {
            LogicalType::String => {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                Values::Str { offsets, data: Vec::with_capacity(capacity * 8) }
            }
            LogicalType::Bool => Values::Bool(BooleanBufferBuilder::new(capacity)),
            LogicalType::Int64 => Values::Int(Vec::with_capacity(capacity)),
            LogicalType::Float64 => Values::Float(Vec::with_capacity(capacity)),
            LogicalType::Decimal { precision, scale } => Values::Decimal {
                values: Vec::with_capacity(capacity),
                precision: *precision,
                scale: *scale,
                limit: 10i128.pow(*precision as u32),
            },
            LogicalType::Date => Values::Date(Vec::with_capacity(capacity)),
            LogicalType::Timestamp { unit, tz } => Values::Ts {
                values: Vec::with_capacity(capacity),
                unit: *unit,
                tz: tz.clone(),
                expect: tz.as_deref().and_then(tz_from_name),
            },
        };
        ColumnBuilder { values, nulls: NullBufferBuilder::new(capacity), len: 0, loose: mode == TypesMode::Loose }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    fn null(&mut self) {
        self.nulls.append_null();
        match &mut self.values {
            Values::Str { offsets, data } => offsets.push(data.len() as i32),
            Values::Bool(b) => b.append(false),
            Values::Int(v) => v.push(0),
            Values::Float(v) => v.push(0.0),
            Values::Decimal { values, .. } => values.push(0),
            Values::Date(v) => v.push(0),
            Values::Ts { values, .. } => values.push(0),
        }
        self.len += 1;
    }

    /// Append one value.  `missing` marks an unquoted empty field.  Returns false
    /// when the value would lose information under the planned type.
    #[inline]
    pub fn push(&mut self, v: &[u8], missing: bool, extra_nulls: &[Vec<u8>]) -> bool {
        if let Values::Str { offsets, data } = &mut self.values {
            if missing {
                self.nulls.append_null();
                offsets.push(data.len() as i32);
            } else {
                self.nulls.append_non_null();
                data.extend_from_slice(v);
                if data.len() > i32::MAX as usize {
                    return false;
                }
                offsets.push(data.len() as i32);
            }
            self.len += 1;
            return true;
        }
        if missing || is_null_token(v, extra_nulls) || (is_nan_token(v) && !matches!(self.values, Values::Float(_))) {
            self.null();
            return true;
        }
        let loose = self.loose;
        let ok = match &mut self.values {
            Values::Str { .. } => unreachable!(),
            Values::Bool(b) => match parse_bool(v) {
                Some(x) => {
                    b.append(x);
                    true
                }
                None => false,
            },
            Values::Int(out) => match parse_int(v, loose) {
                IntParse::Ok(x) => {
                    out.push(x);
                    true
                }
                _ => false,
            },
            Values::Float(out) => {
                if is_nan_token(v) {
                    out.push(f64::NAN);
                    true
                } else {
                    match parse_float(v, loose) {
                        Some(info) if info.lossless => {
                            out.push(info.value);
                            true
                        }
                        _ => false,
                    }
                }
            }
            Values::Decimal { values, scale, limit, .. } => match parse_decimal(v, *scale) {
                Some(x) if x.abs() < *limit => {
                    values.push(x);
                    true
                }
                _ => false,
            },
            Values::Date(out) => match parse_date(v) {
                Some(d) => {
                    out.push(d);
                    true
                }
                None => false,
            },
            Values::Ts { values, unit, expect, .. } => match parse_timestamp(v) {
                Some(ts) => {
                    let tz_ok = match (expect, ts.tz) {
                        (None, Tz::Naive) => true,
                        (Some(e), t) => t == *e,
                        _ => false,
                    };
                    let precise = (ts.frac_digits as u32)
                        <= match unit {
                            Unit::S => 0,
                            Unit::Ms => 3,
                            Unit::Us => 6,
                            Unit::Ns => 9,
                        };
                    match ts.to_unit(*unit) {
                        Some(x) if tz_ok && precise => {
                            values.push(x);
                            true
                        }
                        _ => false,
                    }
                }
                None => false,
            },
        };
        if ok {
            self.nulls.append_non_null();
            self.len += 1;
        }
        ok
    }

    pub fn finish(mut self) -> ArrayRef {
        let nulls = self.nulls.finish();
        match self.values {
            Values::Str { offsets, data } => {
                let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
                // SAFETY: every chunk is validated as UTF-8 before tokenizing, and field
                // boundaries (delimiters, quotes, terminators) are ASCII, so each value
                // is a complete UTF-8 sequence; offsets are monotonic by construction.
                Arc::new(unsafe { StringArray::new_unchecked(offsets, Buffer::from_vec(data), nulls) })
            }
            Values::Bool(mut b) => Arc::new(BooleanArray::new(b.finish(), nulls)),
            Values::Int(v) => Arc::new(Int64Array::new(ScalarBuffer::from(v), nulls)),
            Values::Float(v) => Arc::new(Float64Array::new(ScalarBuffer::from(v), nulls)),
            Values::Decimal { values, precision, scale, .. } => Arc::new(
                Decimal128Array::new(ScalarBuffer::from(values), nulls)
                    .with_precision_and_scale(precision, scale as i8)
                    .expect("valid decimal precision"),
            ),
            Values::Date(v) => Arc::new(Date32Array::new(ScalarBuffer::from(v), nulls)),
            Values::Ts { values, unit, tz, .. } => {
                let values = ScalarBuffer::from(values);
                match unit {
                    Unit::S => Arc::new(TimestampSecondArray::new(values, nulls).with_timezone_opt(tz)),
                    Unit::Ms => Arc::new(TimestampMillisecondArray::new(values, nulls).with_timezone_opt(tz)),
                    Unit::Us => Arc::new(TimestampMicrosecondArray::new(values, nulls).with_timezone_opt(tz)),
                    Unit::Ns => Arc::new(TimestampNanosecondArray::new(values, nulls).with_timezone_opt(tz)),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn builds_typed_arrays() {
        let mut b = ColumnBuilder::new(&LogicalType::Int64, 4, TypesMode::Infer);
        assert!(b.push(b"12", false, &[]));
        assert!(b.push(b"", true, &[]));
        assert!(b.push(b"NA", false, &[]));
        assert!(!b.push(b"007", false, &[]));
        let a = b.finish();
        assert_eq!(a.len(), 3);
        assert_eq!(a.null_count(), 2);

        let mut b = ColumnBuilder::new(&LogicalType::String, 4, TypesMode::Infer);
        assert!(b.push(b"", true, &[]));
        assert!(b.push(b"", false, &[]));
        assert!(b.push(b"NA", false, &[]));
        let a = b.finish();
        let s = a.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(s.is_null(0));
        assert_eq!(s.value(1), "");
        assert_eq!(s.value(2), "NA");

        let ty = LogicalType::Timestamp { unit: Unit::Ms, tz: Some("UTC".into()) };
        let mut b = ColumnBuilder::new(&ty, 2, TypesMode::Infer);
        assert!(b.push(b"1970-01-01T00:00:01.5Z", false, &[]));
        assert!(!b.push(b"1970-01-01T00:00:01", false, &[]), "naive value in a UTC column");
        assert!(!b.push(b"1970-01-01T00:00:01.1234Z", false, &[]), "more precision than ms");
        let a = b.finish();
        let t = a.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
        assert_eq!(t.value(0), 1500);

        let mut b = ColumnBuilder::new(&LogicalType::Decimal { precision: 5, scale: 2 }, 2, TypesMode::Infer);
        assert!(b.push(b"123.4", false, &[]));
        assert!(!b.push(b"1234.5", false, &[]), "exceeds precision");
        assert!(!b.push(b"1.234", false, &[]), "exceeds scale");
        let a = b.finish();
        assert_eq!(a.as_any().downcast_ref::<Decimal128Array>().unwrap().value(0), 12340);

        let mut b = ColumnBuilder::new(&LogicalType::Float64, 2, TypesMode::Infer);
        assert!(b.push(b"NaN", false, &[]));
        assert!(!b.push(b"1.00000000000000000001", false, &[]));
        assert!(b.finish().as_any().downcast_ref::<Float64Array>().unwrap().value(0).is_nan());
    }
}
