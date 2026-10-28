//! JSON Lines with lossless types.
//!
//! Column types come from the values themselves, with keys in first-seen order:
//!
//! * integer literals that fit int64 are int64; longer ones are decimal(p, 0) up to 38
//!   digits, and beyond that their exact text;
//! * other numbers are float64 when every value survives the round trip (the CSV
//!   rule: at most 15 significant digits, or 16-17 that parse back exactly), else
//!   decimal(p, s) for plain notation up to 38 digits, else their text (as written,
//!   except that an exponent becomes `e` with an explicit sign: `1E5` is `1e+5`); the
//!   strings "NaN", "Infinity" and "-Infinity" count as floats in a number field;
//! * objects become structs and arrays lists, recursively;
//! * a field that mixes strings, numbers, booleans and objects keeps every value as
//!   its JSON text, strings with their quotes, so `1` and `"1"` stay different.
//!
//! The decoder checks every value against its column.  A value beyond the inference
//! sample that does not fit, or a key first seen there, fails the job with
//! [`Error::Replan`]; the caller infers again from the whole file and starts over.
//!
//! Writing goes through arrow-json, except that non-finite floats are written as the
//! strings above (JSON has no NaN), so they read back as floats.

use std::io::Read;
use std::sync::Arc;

use ahash::AHashMap;
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, AsArray, BooleanBuilder, Decimal128Builder, Float64Builder, Int64Builder, ListArray,
    RecordBatch, RecordBatchOptions, StringBuilder, StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Float32Type, Float64Type, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::json::writer::{Encoder, EncoderFactory, EncoderOptions, NullableEncoder};
use rayon::prelude::*;
use serde_json::Value;

use crate::chunk::Chunker;
use crate::error::{Error, Result};
use crate::types::{parse_decimal, parse_float};

fn nan_text(s: &str) -> Option<f64> {
    match s {
        "NaN" => Some(f64::NAN),
        "Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        _ => None,
    }
}

// ---------------------------------------------------------------------------------
// Inference
// ---------------------------------------------------------------------------------

/// What the numbers of a field have looked like.
#[derive(Clone, Copy, Debug, Default)]
struct Num {
    /// Some literal has a fraction or an exponent, or is -0 (an integer loses the sign).
    frac: bool,
    /// Some integer literal does not fit int64.
    wide: bool,
    /// Some literal does not survive float64.
    lossy: bool,
    /// Some literal has no decimal form (exponent notation, negative zero).
    no_decimal: bool,
    /// Widest integer part and fraction of the plain literals.
    int_digits: u16,
    scale: u16,
    /// "NaN" / "Infinity" / "-Infinity" strings were seen.
    nan: bool,
}

impl Num {
    fn of(text: &str) -> Num {
        let b = text.as_bytes();
        let exp = b.iter().any(|&c| c == b'e' || c == b'E');
        let neg_zero = b.first() == Some(&b'-') && b[1..].iter().all(|&c| c == b'0' || c == b'.');
        let integer = !exp && !b.contains(&b'.') && !neg_zero;
        let mut n = Num {
            frac: !integer,
            wide: integer && text.parse::<i64>().is_err(),
            lossy: !parse_float(b, false).is_some_and(|f| f.lossless),
            no_decimal: exp || neg_zero,
            ..Num::default()
        };
        if !exp {
            let body = text.trim_start_matches('-');
            let (ip, fp) = body.split_once('.').unwrap_or((body, ""));
            n.int_digits = ip.trim_start_matches('0').len().min(u16::MAX as usize) as u16;
            n.scale = fp.len().min(u16::MAX as usize) as u16;
        }
        n
    }

    fn merge(&mut self, o: &Num) {
        self.frac |= o.frac;
        self.wide |= o.wide;
        self.lossy |= o.lossy;
        self.no_decimal |= o.no_decimal;
        self.int_digits = self.int_digits.max(o.int_digits);
        self.scale = self.scale.max(o.scale);
        self.nan |= o.nan;
    }
}

#[derive(Clone, Debug)]
enum Kind {
    Null,
    Bool,
    Num(Num),
    /// Only the strings "NaN", "Infinity" and "-Infinity".
    NanText,
    Str,
    Obj(Keys),
    Arr(Box<Kind>),
    Mixed,
}

/// The fields of an object in first-seen order.
#[derive(Clone, Debug, Default)]
struct Keys {
    names: Vec<String>,
    kinds: Vec<Kind>,
    index: AHashMap<String, usize>,
}

impl Keys {
    fn absorb(&mut self, key: &str, v: &Value) {
        match self.index.get(key) {
            Some(&i) => absorb(&mut self.kinds[i], v),
            None => self.push(key.to_owned(), observe(v)),
        }
    }

    fn push(&mut self, name: String, kind: Kind) {
        self.index.insert(name.clone(), self.names.len());
        self.names.push(name);
        self.kinds.push(kind);
    }

    fn merge(&mut self, other: Keys) {
        for (name, kind) in other.names.into_iter().zip(other.kinds) {
            match self.index.get(&name) {
                Some(&i) => merge(&mut self.kinds[i], kind),
                None => self.push(name, kind),
            }
        }
    }
}

fn observe(v: &Value) -> Kind {
    match v {
        Value::Null => Kind::Null,
        Value::Bool(_) => Kind::Bool,
        Value::Number(n) => Kind::Num(Num::of(n.as_str())),
        Value::String(s) if nan_text(s).is_some() => Kind::NanText,
        Value::String(_) => Kind::Str,
        Value::Object(m) => {
            let mut keys = Keys::default();
            for (k, x) in m {
                keys.absorb(k, x);
            }
            Kind::Obj(keys)
        }
        Value::Array(a) => {
            let mut item = Kind::Null;
            for x in a {
                absorb(&mut item, x);
            }
            Kind::Arr(Box::new(item))
        }
    }
}

/// Fold one more value into what a field has looked like.
fn absorb(k: &mut Kind, v: &Value) {
    if v.is_null() || matches!(k, Kind::Mixed) {
        return;
    }
    match (&mut *k, v) {
        (Kind::Obj(keys), Value::Object(m)) => {
            for (key, x) in m {
                keys.absorb(key, x);
            }
            return;
        }
        (Kind::Arr(item), Value::Array(a)) => {
            for x in a {
                absorb(item, x);
            }
            return;
        }
        (Kind::Num(n), Value::Number(x)) => {
            n.merge(&Num::of(x.as_str()));
            return;
        }
        (Kind::Bool, Value::Bool(_)) | (Kind::Str, Value::String(_)) => return,
        _ => {}
    }
    merge(k, observe(v));
}

fn merge(a: &mut Kind, b: Kind) {
    if matches!(b, Kind::Null) || matches!(a, Kind::Mixed) {
        return;
    }
    if matches!(a, Kind::Null) {
        *a = b;
        return;
    }
    if matches!(a, Kind::NanText) {
        *a = match b {
            Kind::Num(mut n) => {
                n.nan = true;
                Kind::Num(n)
            }
            Kind::NanText => Kind::NanText,
            Kind::Str => Kind::Str,
            _ => Kind::Mixed,
        };
        return;
    }
    let fits = match (&mut *a, b) {
        (Kind::Bool, Kind::Bool) | (Kind::Str, Kind::Str) | (Kind::Str, Kind::NanText) => true,
        (Kind::Num(x), Kind::Num(y)) => {
            x.merge(&y);
            true
        }
        (Kind::Num(x), Kind::NanText) => {
            x.nan = true;
            true
        }
        (Kind::Obj(x), Kind::Obj(y)) => {
            x.merge(y);
            true
        }
        (Kind::Arr(x), Kind::Arr(y)) => {
            merge(x, *y);
            true
        }
        _ => false,
    };
    if !fits {
        *a = Kind::Mixed;
    }
}

fn absorb_lines(data: &[u8], offset: u64) -> Result<Keys> {
    let mut keys = Keys::default();
    let mut pos = 0usize;
    for line in data.split(|&b| b == b'\n') {
        let at = offset + pos as u64;
        pos += line.len() + 1;
        if line.iter().all(|b| b.is_ascii_whitespace()) {
            continue;
        }
        match parse_line(line, at)? {
            Value::Object(m) => {
                for (k, x) in &m {
                    keys.absorb(k, x);
                }
            }
            _ => return Err(not_object(at)),
        }
    }
    Ok(keys)
}

fn parse_line(line: &[u8], at: u64) -> Result<Value> {
    serde_json::from_slice(line).map_err(|e| Error::Data(format!("invalid JSON in the line at byte {at}: {e}")))
}

fn not_object(at: u64) -> Error {
    Error::Data(format!("the JSON Lines record at byte {at} is not an object"))
}

/// How one column is decoded.
#[derive(Clone, Debug)]
enum Plan {
    Int,
    Float,
    Decimal(u8, i8),
    Text,
    /// Numbers (and NaN strings) kept as their literal text.
    NumText,
    /// Any value as its JSON text.
    Json,
    Bool,
    Struct(Arc<Layout>),
    List(Box<Plan>),
}

impl Plan {
    fn data_type(&self) -> DataType {
        match self {
            Plan::Int => DataType::Int64,
            Plan::Float => DataType::Float64,
            Plan::Decimal(p, s) => DataType::Decimal128(*p, *s),
            Plan::Text | Plan::NumText | Plan::Json => DataType::Utf8,
            Plan::Bool => DataType::Boolean,
            Plan::Struct(l) => DataType::Struct(l.fields()),
            Plan::List(item) => DataType::List(Arc::new(Field::new("item", item.data_type(), true))),
        }
    }
}

/// The columns of an object: names, decode plans and a lookup by name.
#[derive(Debug)]
pub struct Layout {
    names: Vec<String>,
    plans: Vec<Plan>,
    index: AHashMap<String, usize>,
}

impl Layout {
    fn fields(&self) -> Fields {
        self.names.iter().zip(&self.plans).map(|(n, p)| Field::new(n, p.data_type(), true)).collect()
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(self.fields()))
    }
}

fn num_plan(n: &Num) -> Plan {
    if !n.frac && !n.nan {
        return if !n.wide {
            Plan::Int
        } else if n.int_digits <= 38 {
            Plan::Decimal(n.int_digits.max(1) as u8, 0)
        } else {
            Plan::NumText
        };
    }
    if !n.lossy {
        return Plan::Float;
    }
    let digits = n.int_digits as usize + n.scale as usize;
    if !n.nan && !n.no_decimal && (1..=38).contains(&digits) {
        return Plan::Decimal(digits as u8, n.scale as i8);
    }
    Plan::NumText
}

fn plan(kind: &Kind, path: &str, warnings: &mut Vec<String>) -> Plan {
    match kind {
        Kind::Null | Kind::Str | Kind::NanText => Plan::Text,
        Kind::Bool => Plan::Bool,
        Kind::Num(n) => num_plan(n),
        Kind::Mixed => {
            warnings.push(format!("column '{path}' mixes JSON types; values are stored as JSON text"));
            Plan::Json
        }
        // an object that never has a field cannot be a column of its own
        Kind::Obj(keys) if keys.names.is_empty() => Plan::Json,
        Kind::Obj(keys) => Plan::Struct(Arc::new(layout(keys, path, warnings))),
        Kind::Arr(item) => Plan::List(Box::new(plan(item, &format!("{path}[]"), warnings))),
    }
}

fn layout(keys: &Keys, path: &str, warnings: &mut Vec<String>) -> Layout {
    let plans = keys
        .names
        .iter()
        .zip(&keys.kinds)
        .map(|(name, kind)| {
            let p = if path.is_empty() { name.clone() } else { format!("{path}.{name}") };
            plan(kind, &p, warnings)
        })
        .collect();
    Layout { names: keys.names.clone(), plans, index: keys.index.clone() }
}

/// Infer the columns of the JSON Lines in `reader`, from at most `limit` bytes of it
/// (whole lines).  Returns the layout and warnings about mixed-type columns.
pub fn infer(reader: Box<dyn Read + Send>, limit: Option<u64>) -> Result<(Layout, Vec<String>)> {
    // pieces no larger than the sample, so that a sample means what it says
    let piece = limit.map_or(4 << 20, |l| (l as usize).clamp(64 << 10, 4 << 20));
    let mut chunker = Chunker::new(crate::io::SkipBom::new(reader), piece, None, b'\n');
    let threads = rayon::current_num_threads().max(1);
    let mut keys = Keys::default();
    let mut seen = 0u64;
    loop {
        let mut wave = Vec::with_capacity(threads);
        while wave.len() < threads && limit.is_none_or(|l| seen < l) {
            match chunker.next_chunk()? {
                Some(c) => {
                    seen += c.data.len() as u64;
                    wave.push(c);
                }
                None => break,
            }
        }
        if wave.is_empty() {
            break;
        }
        let parts: Vec<Keys> = wave.par_iter().map(|c| absorb_lines(&c.data, c.offset)).collect::<Result<_>>()?;
        for p in parts {
            keys.merge(p);
        }
        if limit.is_some_and(|l| seen >= l) {
            break;
        }
    }
    let mut warnings = Vec::new();
    let layout = layout(&keys, "", &mut warnings);
    Ok((layout, warnings))
}

// ---------------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum TextMode {
    Text,
    NumText,
    Json,
}

enum Builder {
    Int(Int64Builder),
    Float(Float64Builder),
    Decimal(Decimal128Builder, u8, i8),
    Text(StringBuilder, TextMode),
    Bool(BooleanBuilder),
    Struct(Arc<Layout>, Vec<Builder>, Vec<bool>),
    List(Box<Builder>, Vec<i32>, Vec<bool>, DataType),
}

/// A short description of a value that does not fit its column.
fn shown(v: &Value) -> String {
    let mut s = serde_json::to_string(v).unwrap_or_default();
    if s.len() > 60 {
        let mut cut = 57;
        while !s.is_char_boundary(cut) {
            cut -= 1;
        }
        s.truncate(cut);
        s.push_str("...");
    }
    s
}

impl Builder {
    fn new(p: &Plan) -> Builder {
        match p {
            Plan::Int => Builder::Int(Int64Builder::new()),
            Plan::Float => Builder::Float(Float64Builder::new()),
            Plan::Decimal(p, s) => Builder::Decimal(Decimal128Builder::new(), *p, *s),
            Plan::Text => Builder::Text(StringBuilder::new(), TextMode::Text),
            Plan::NumText => Builder::Text(StringBuilder::new(), TextMode::NumText),
            Plan::Json => Builder::Text(StringBuilder::new(), TextMode::Json),
            Plan::Bool => Builder::Bool(BooleanBuilder::new()),
            Plan::Struct(l) => Builder::Struct(l.clone(), l.plans.iter().map(Builder::new).collect(), Vec::new()),
            Plan::List(item) => Builder::List(Box::new(Builder::new(item)), vec![0], Vec::new(), item.data_type()),
        }
    }

    fn len(&self) -> usize {
        match self {
            Builder::Int(b) => b.len(),
            Builder::Float(b) => b.len(),
            Builder::Decimal(b, ..) => b.len(),
            Builder::Text(b, _) => b.len(),
            Builder::Bool(b) => b.len(),
            Builder::Struct(_, _, valid) | Builder::List(_, _, valid, _) => valid.len(),
        }
    }

    fn append_null(&mut self) {
        match self {
            Builder::Int(b) => b.append_null(),
            Builder::Float(b) => b.append_null(),
            Builder::Decimal(b, ..) => b.append_null(),
            Builder::Text(b, _) => b.append_null(),
            Builder::Bool(b) => b.append_null(),
            Builder::Struct(_, children, valid) => {
                for c in children {
                    c.append_null();
                }
                valid.push(false);
            }
            Builder::List(_, offsets, valid, _) => {
                offsets.push(*offsets.last().expect("offsets start at 0"));
                valid.push(false);
            }
        }
    }

    /// Append a value, or describe it when it does not fit the column.
    fn append(&mut self, v: Option<&Value>) -> std::result::Result<(), String> {
        let v = match v {
            None | Some(Value::Null) => {
                self.append_null();
                return Ok(());
            }
            Some(v) => v,
        };
        match (self, v) {
            (Builder::Int(b), Value::Number(n)) => match n.as_str().parse::<i64>() {
                Ok(x) if n.as_str() != "-0" => b.append_value(x),
                _ => return Err(shown(v)),
            },
            (Builder::Float(b), Value::Number(n)) => match parse_float(n.as_str().as_bytes(), false) {
                Some(f) if f.lossless => b.append_value(f.value),
                _ => return Err(shown(v)),
            },
            (Builder::Float(b), Value::String(s)) => match nan_text(s) {
                Some(x) => b.append_value(x),
                None => return Err(shown(v)),
            },
            (Builder::Decimal(b, p, s), Value::Number(n)) => match parse_decimal(n.as_str().as_bytes(), *s as u8) {
                Some(x) if x.unsigned_abs() < 10u128.pow(*p as u32) => b.append_value(x),
                _ => return Err(shown(v)),
            },
            (Builder::Text(b, TextMode::Text), Value::String(s)) => b.append_value(s),
            (Builder::Text(b, TextMode::NumText), Value::Number(n)) => b.append_value(n.as_str()),
            (Builder::Text(b, TextMode::NumText), Value::String(s)) if nan_text(s).is_some() => b.append_value(s),
            (Builder::Text(b, TextMode::Json), v) => b.append_value(serde_json::to_string(v).unwrap_or_default()),
            (Builder::Bool(b), Value::Bool(x)) => b.append_value(*x),
            (Builder::Struct(layout, children, valid), Value::Object(m)) => {
                if let Some(key) = m.keys().find(|k| !layout.index.contains_key(k.as_str())) {
                    return Err(format!("with the new key \"{key}\""));
                }
                for (name, child) in layout.names.iter().zip(children.iter_mut()) {
                    child.append(m.get(name))?;
                }
                valid.push(true);
            }
            (Builder::List(child, offsets, valid, _), Value::Array(a)) => {
                for x in a {
                    child.append(Some(x))?;
                }
                let len = i32::try_from(child.len()).map_err(|_| "a list too long for one chunk".to_string())?;
                offsets.push(len);
                valid.push(true);
            }
            _ => return Err(shown(v)),
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<ArrayRef> {
        Ok(match self {
            Builder::Int(b) => Arc::new(b.finish()),
            Builder::Float(b) => Arc::new(b.finish()),
            Builder::Decimal(b, p, s) => Arc::new(b.finish().with_precision_and_scale(*p, *s)?),
            Builder::Text(b, _) => Arc::new(b.finish()),
            Builder::Bool(b) => Arc::new(b.finish()),
            Builder::Struct(layout, children, valid) => {
                let arrays = children.iter_mut().map(|c| c.finish()).collect::<Result<Vec<_>>>()?;
                let nulls = NullBuffer::from(std::mem::take(valid));
                Arc::new(StructArray::try_new(layout.fields(), arrays, Some(nulls))?)
            }
            Builder::List(child, offsets, valid, item) => {
                let values = child.finish()?;
                let offsets = OffsetBuffer::new(ScalarBuffer::from(std::mem::replace(offsets, vec![0])));
                let nulls = NullBuffer::from(std::mem::take(valid));
                Arc::new(ListArray::try_new(
                    Arc::new(Field::new("item", item.clone(), true)),
                    offsets,
                    values,
                    Some(nulls),
                )?)
            }
        })
    }
}

/// Decode whole lines of JSON into one batch (None when there are no records).
pub fn decode(layout: &Layout, schema: &SchemaRef, data: &[u8], offset: u64) -> Result<Option<RecordBatch>> {
    let mut cols: Vec<Builder> = layout.plans.iter().map(Builder::new).collect();
    let mut rows = 0usize;
    let mut pos = 0usize;
    for line in data.split(|&b| b == b'\n') {
        let at = offset + pos as u64;
        pos += line.len() + 1;
        if line.iter().all(|b| b.is_ascii_whitespace()) {
            continue;
        }
        let Value::Object(m) = parse_line(line, at)? else { return Err(not_object(at)) };
        if let Some(key) = m.keys().find(|k| !layout.index.contains_key(k.as_str())) {
            return Err(Error::Replan(format!("key '{key}' first appears beyond the inference sample")));
        }
        for ((name, b), p) in layout.names.iter().zip(cols.iter_mut()).zip(&layout.plans) {
            b.append(m.get(name)).map_err(|value| {
                Error::Replan(format!("column '{name}': value {value} does not fit {}", p.data_type()))
            })?;
        }
        rows += 1;
    }
    if rows == 0 {
        return Ok(None);
    }
    let arrays = cols.iter_mut().map(|b| b.finish()).collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    Ok(Some(RecordBatch::try_new_with_options(schema.clone(), arrays, &options)?))
}

/// Infer and decode JSON Lines held in memory (previews).
pub fn read_text(text: &[u8]) -> Result<Option<RecordBatch>> {
    let (layout, _) = infer(Box::new(std::io::Cursor::new(text.to_vec())), None)?;
    let schema = layout.schema();
    let start = usize::from(text.starts_with(b"\xEF\xBB\xBF")) * 3;
    decode(&layout, &schema, &text[start..], 0)
}

// ---------------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------------

/// Writes non-finite floats as "NaN", "Infinity" and "-Infinity" (JSON has no such
/// numbers and arrow-json would write null).
#[derive(Debug)]
pub struct FloatText;

struct FloatEncoder<'a>(&'a dyn Array);

impl Encoder for FloatEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let v = match self.0.data_type() {
            DataType::Float32 => self.0.as_primitive::<Float32Type>().value(idx) as f64,
            _ => self.0.as_primitive::<Float64Type>().value(idx),
        };
        if v.is_nan() {
            out.extend_from_slice(b"\"NaN\"");
        } else if v.is_infinite() {
            out.extend_from_slice(if v > 0.0 { b"\"Infinity\"" } else { b"\"-Infinity\"" });
        } else if self.0.data_type() == &DataType::Float32 {
            let mut buf = ryu::Buffer::new();
            out.extend_from_slice(buf.format_finite(self.0.as_primitive::<Float32Type>().value(idx)).as_bytes());
        } else {
            let mut buf = ryu::Buffer::new();
            out.extend_from_slice(buf.format_finite(v).as_bytes());
        }
    }
}

impl EncoderFactory for FloatText {
    fn make_default_encoder<'a>(
        &self,
        _field: &'a FieldRef,
        array: &'a dyn Array,
        _options: &'a EncoderOptions,
    ) -> std::result::Result<Option<NullableEncoder<'a>>, ArrowError> {
        Ok(match array.data_type() {
            DataType::Float32 | DataType::Float64 => {
                Some(NullableEncoder::new(Box::new(FloatEncoder(array)), array.nulls().cloned()))
            }
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Decimal128Array, Float64Array, Int64Array, StringArray};

    fn read(text: &str) -> RecordBatch {
        read_text(text.as_bytes()).unwrap().unwrap()
    }

    #[test]
    fn numbers_are_lossless() {
        let b = read(concat!(
            "{\"id\": 12345678901234567890, \"n\": 1, \"x\": 0.1, \"p\": 1.00000000000000000001, \"e\": 1e400}\n",
            "{\"id\": 1, \"n\": -2, \"x\": 2.5, \"p\": 2, \"e\": 1}\n",
        ));
        let types: Vec<String> = b.schema().fields().iter().map(|f| f.data_type().to_string()).collect();
        assert_eq!(types, ["Decimal128(20, 0)", "Int64", "Float64", "Decimal128(21, 20)", "Utf8"]);
        let id = b.column(0).as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(id.value_as_string(0), "12345678901234567890");
        // kept as text: exponents are written with a lower-case `e` and a sign
        assert_eq!(b.column(4).as_string::<i32>().value(0), "1e+400");
        assert_eq!(b.column(1).as_any().downcast_ref::<Int64Array>().unwrap().value(1), -2);
    }

    #[test]
    fn order_mixing_and_nesting() {
        let b = read(concat!(
            "\u{feff}{\"zeta\": 1, \"alpha\": \"a\", \"mix\": 1, \"obj\": {\"k\": [1, 2]}, \"f\": \"NaN\"}\n",
            "{\"zeta\": 2, \"mix\": \"1\", \"obj\": null, \"f\": 1.5, \"late\": true}\n",
        ));
        let names: Vec<String> = b.schema().fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, ["zeta", "alpha", "mix", "obj", "f", "late"]);
        let mix = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!((mix.value(0), mix.value(1)), ("1", "\"1\""));
        let f = b.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(f.value(0).is_nan());
        assert!(b.column(3).is_null(1));
        assert!(b.column(5).is_null(0));
    }

    #[test]
    fn values_beyond_the_sample_ask_for_a_replan() {
        let (layout, _) = infer(Box::new(std::io::Cursor::new(b"{\"a\": 1}\n".to_vec())), None).unwrap();
        let schema = layout.schema();
        assert!(decode(&layout, &schema, b"{\"a\": 2}\n", 0).unwrap().is_some());
        for line in ["{\"a\": 1.5}", "{\"a\": \"x\"}", "{\"a\": 1, \"b\": 2}", "{\"a\": 99999999999999999999}"] {
            assert!(matches!(decode(&layout, &schema, line.as_bytes(), 0), Err(Error::Replan(_))), "{line}");
        }
        assert!(matches!(decode(&layout, &schema, b"[1]\n", 0), Err(Error::Data(_))));
    }
}
