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
//! strings above (JSON has no NaN), so they read back as floats, dates and timestamps
//! are written exactly as in CSV output, and text columns marked [`JSON_TEXT`] (numbers
//! kept as text, mixed-type fields) are written as the JSON they hold, so a round trip
//! through Parquet or Arrow gives back the same JSON.

use std::borrow::Cow;
use std::io::Read;
use std::sync::Arc;

use ahash::AHashMap;
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, AsArray, BooleanBuilder, Decimal128Builder, Float64Builder, Int64Builder, ListArray,
    ListLikeArray, RecordBatch, RecordBatchOptions, StringArrayType, StringBuilder, StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Field, FieldRef, Fields, Float32Type, Float64Type, Schema, SchemaRef,
};
use arrow::error::ArrowError;
use arrow::json::writer::{Encoder, EncoderFactory, EncoderOptions, NullableEncoder};
use rayon::prelude::*;
use serde::de::{Deserialize, Deserializer, MapAccess, Visitor};
use serde_json::value::RawValue;
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

    /// Fold in a value given as raw JSON text (scalars need no parsed tree).
    fn absorb_raw(&mut self, key: &str, raw: &str) {
        let kind = match raw.as_bytes().first() {
            Some(b'{' | b'[') => match serde_json::from_str::<Value>(raw) {
                Ok(v) => return self.absorb(key, &v),
                Err(_) => Kind::Mixed,
            },
            Some(b'"') => match string_text(raw) {
                Some(s) if nan_text(&s).is_some() => Kind::NanText,
                _ => Kind::Str,
            },
            Some(b't' | b'f') => Kind::Bool,
            Some(b'n') => Kind::Null,
            _ => Kind::Num(Num::of(raw)),
        };
        match self.index.get(key) {
            Some(&i) => merge(&mut self.kinds[i], kind),
            None => self.push(key.to_owned(), kind),
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
        let record: RawRecord = serde_json::from_slice(line).map_err(|e| invalid_line(e, at))?;
        for (key, raw) in &record.0 {
            keys.absorb_raw(&key.0, raw.get());
        }
    }
    Ok(keys)
}

fn invalid_line(e: serde_json::Error, at: u64) -> Error {
    match e.classify() {
        serde_json::error::Category::Data => not_object(at),
        _ => Error::Data(format!("invalid JSON in the line at byte {at}: {e}")),
    }
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
            Plan::List(item) => DataType::List(Arc::new(plan_field("item", item))),
        }
    }
}

/// Field metadata of text columns that hold JSON: "number" (numbers kept as their
/// text) or "value" (the JSON text of any value).
pub const JSON_TEXT: &str = "dataconv.json";

fn plan_field(name: &str, p: &Plan) -> Field {
    let field = Field::new(name, p.data_type(), true);
    let mark = |kind: &str| std::collections::HashMap::from([(JSON_TEXT.to_string(), kind.to_string())]);
    match p {
        Plan::NumText => field.with_metadata(mark("number")),
        Plan::Json => field.with_metadata(mark("value")),
        _ => field,
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
        self.names.iter().zip(&self.plans).map(|(n, p)| plan_field(n, p)).collect()
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
    List(Box<Builder>, Vec<i32>, Vec<bool>, FieldRef),
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
            Plan::List(item) => {
                Builder::List(Box::new(Builder::new(item)), vec![0], Vec::new(), Arc::new(plan_field("item", item)))
            }
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
                Arc::new(ListArray::try_new(item.clone(), offsets, values, Some(nulls))?)
            }
        })
    }
}

/// A key, borrowed from the line unless it holds escapes.
struct Key<'de>(Cow<'de, str>);

impl<'de> Deserialize<'de> for Key<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct KeyVisitor;
        impl<'de> Visitor<'de> for KeyVisitor {
            type Value = Key<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a key")
            }
            fn visit_borrowed_str<E>(self, v: &'de str) -> std::result::Result<Key<'de>, E> {
                Ok(Key(Cow::Borrowed(v)))
            }
            fn visit_str<E>(self, v: &str) -> std::result::Result<Key<'de>, E> {
                Ok(Key(Cow::Owned(v.to_owned())))
            }
        }
        d.deserialize_str(KeyVisitor)
    }
}

/// The fields of one record in file order, each value as its raw JSON text: the
/// parser only checks the syntax and finds where values end, and the columns read
/// the text themselves (exactly, and without building a tree per value).
struct RawRecord<'de>(Vec<(Key<'de>, &'de RawValue)>);

impl<'de> Deserialize<'de> for RawRecord<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct RecordVisitor;
        impl<'de> Visitor<'de> for RecordVisitor {
            type Value = RawRecord<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<RawRecord<'de>, A::Error> {
                let mut fields = Vec::with_capacity(map.size_hint().unwrap_or(16));
                while let Some(key) = map.next_key::<Key<'de>>()? {
                    fields.push((key, map.next_value::<&'de RawValue>()?));
                }
                Ok(RawRecord(fields))
            }
        }
        d.deserialize_map(RecordVisitor)
    }
}

/// The text of a JSON number value, or None for other values.
fn number_text(raw: &str) -> Option<&str> {
    raw.as_bytes().first().is_some_and(|&b| b == b'-' || b.is_ascii_digit()).then_some(raw)
}

/// The content of a JSON string value (unescaped), or None for other values.
fn string_text(raw: &str) -> Option<Cow<'_, str>> {
    let inner = raw.strip_prefix('"')?.strip_suffix('"')?;
    if inner.contains('\\') {
        serde_json::from_str::<String>(raw).ok().map(Cow::Owned)
    } else {
        Some(Cow::Borrowed(inner))
    }
}

/// A number's text as numbers kept as text are written (see the module notes): an
/// exponent with a lower-case `e` and an explicit sign.
fn canonical_number(t: &str) -> Cow<'_, str> {
    match t.find(['e', 'E']) {
        None => Cow::Borrowed(t),
        Some(i) => {
            let (mantissa, rest) = (&t[..i], &t[i + 1..]);
            let sign = if rest.starts_with(['+', '-']) { "" } else { "+" };
            Cow::Owned(format!("{mantissa}e{sign}{rest}"))
        }
    }
}

/// A short description of a raw value that does not fit its column.
fn shown_raw(raw: &str) -> String {
    match serde_json::from_str::<Value>(raw) {
        Ok(v) => shown(&v),
        Err(_) => raw.chars().take(60).collect(),
    }
}

impl Builder {
    /// Append a value given as raw JSON text (None: the key is absent).
    fn append_raw(&mut self, raw: Option<&str>) -> std::result::Result<(), String> {
        let Some(raw) = raw.filter(|r| *r != "null") else {
            self.append_null();
            return Ok(());
        };
        let fits = match self {
            Builder::Int(b) => match number_text(raw).map(|t| (t, t.parse::<i64>())) {
                Some((t, Ok(x))) if t != "-0" => {
                    b.append_value(x);
                    true
                }
                _ => false,
            },
            Builder::Float(b) => match number_text(raw) {
                Some(t) => match parse_float(t.as_bytes(), false) {
                    Some(f) if f.lossless => {
                        b.append_value(f.value);
                        true
                    }
                    _ => false,
                },
                None => match string_text(raw).as_deref().and_then(nan_text) {
                    Some(x) => {
                        b.append_value(x);
                        true
                    }
                    None => false,
                },
            },
            Builder::Decimal(b, p, s) => match number_text(raw).and_then(|t| parse_decimal(t.as_bytes(), *s as u8)) {
                Some(x) if x.unsigned_abs() < 10u128.pow(*p as u32) => {
                    b.append_value(x);
                    true
                }
                _ => false,
            },
            Builder::Text(b, TextMode::Text) => match string_text(raw) {
                Some(s) => {
                    b.append_value(&*s);
                    true
                }
                None => false,
            },
            Builder::Text(b, TextMode::NumText) => match number_text(raw) {
                Some(t) => {
                    b.append_value(&*canonical_number(t));
                    true
                }
                None => match string_text(raw) {
                    Some(s) if nan_text(&s).is_some() => {
                        b.append_value(&*s);
                        true
                    }
                    _ => false,
                },
            },
            Builder::Bool(b) => match raw {
                "true" | "false" => {
                    b.append_value(raw == "true");
                    true
                }
                _ => false,
            },
            // mixed-type and nested values go through a parsed tree
            Builder::Text(_, TextMode::Json) | Builder::Struct(..) | Builder::List(..) => {
                let v: Value = serde_json::from_str(raw).map_err(|e| e.to_string())?;
                return self.append(Some(&v));
            }
        };
        if fits {
            Ok(())
        } else {
            Err(shown_raw(raw))
        }
    }
}

/// Decode whole lines of JSON into one batch (None when there are no records).
pub fn decode(layout: &Layout, schema: &SchemaRef, data: &[u8], offset: u64) -> Result<Option<RecordBatch>> {
    let mut cols: Vec<Builder> = layout.plans.iter().map(Builder::new).collect();
    // where each column's value sits in the current line (the last of duplicate keys)
    let mut slots: Vec<Option<(usize, usize)>> = vec![None; cols.len()];
    let mut rows = 0usize;
    let mut pos = 0usize;
    for line in data.split(|&b| b == b'\n') {
        let at = offset + pos as u64;
        pos += line.len() + 1;
        if line.iter().all(|b| b.is_ascii_whitespace()) {
            continue;
        }
        let record: RawRecord = serde_json::from_slice(line).map_err(|e| invalid_line(e, at))?;
        slots.fill(None);
        for (key, raw) in &record.0 {
            let Some(&i) = layout.index.get(key.0.as_ref()) else {
                return Err(Error::Replan(format!("key '{}' first appears beyond the inference sample", key.0)));
            };
            let text = raw.get();
            slots[i] = Some((text.as_ptr() as usize - line.as_ptr() as usize, text.len()));
        }
        // the line is valid JSON, hence UTF-8, and every slot holds a whole value
        let text = std::str::from_utf8(line).map_err(|_| not_object(at))?;
        for (((name, b), p), slot) in layout.names.iter().zip(cols.iter_mut()).zip(&layout.plans).zip(&slots) {
            b.append_raw(slot.map(|(s, l)| &text[s..s + l])).map_err(|value| {
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
/// numbers and arrow-json would write null), and dates and timestamps as CSV output
/// writes them.
#[derive(Debug)]
pub struct FloatText;

/// Text that holds JSON, written as it is ("NaN"-like texts of a number column
/// stay strings: JSON has no such numbers).
struct RawJson<T>(T, bool);

impl<'a, T: StringArrayType<'a>> Encoder for RawJson<T> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let v = self.0.value(idx);
        if self.1 && nan_text(v).is_some() {
            out.push(b'"');
            out.extend_from_slice(v.as_bytes());
            out.push(b'"');
        } else {
            out.extend_from_slice(v.as_bytes());
        }
    }
}

/// A list whose items are marked [`JSON_TEXT`].  arrow-json hands an item encoder the
/// list's own field, whose metadata says nothing about the items.
struct MarkedItems<'a, L> {
    list: &'a L,
    items: NullableEncoder<'a>,
}

impl<L: ListLikeArray> Encoder for MarkedItems<'_, L> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.push(b'[');
        for (n, i) in self.list.element_range(idx).enumerate() {
            if n > 0 {
                out.push(b',');
            }
            if self.items.is_null(i) {
                out.extend_from_slice(b"null");
            } else {
                self.items.encode(i, out);
            }
        }
        out.push(b']');
    }
}

fn marked_items<'a, L: ListLikeArray>(
    item: &'a FieldRef,
    list: &'a L,
    options: &'a EncoderOptions,
) -> std::result::Result<NullableEncoder<'a>, ArrowError> {
    let items = arrow::json::writer::make_encoder(item, list.values().as_ref(), options)?;
    Ok(NullableEncoder::new(Box::new(MarkedItems { list, items }), list.nulls().cloned()))
}

/// Maps as JSON objects, entries in order (repeated keys included).  A key that is not
/// text is written as the text of its JSON (`1` becomes `"1"`): arrow-json refuses such
/// keys, and hands key and value encoders the map's own field, so it would also lose
/// the marks of the values.
struct MapObject<'a> {
    offsets: &'a [i32],
    keys: NullableEncoder<'a>,
    values: NullableEncoder<'a>,
    key: Vec<u8>,
}

impl Encoder for MapObject<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let (start, end) = (self.offsets[idx] as usize, self.offsets[idx + 1] as usize);
        out.push(b'{');
        for i in start..end {
            if i > start {
                out.push(b',');
            }
            self.key.clear();
            if self.keys.is_null(i) {
                self.key.extend_from_slice(b"null");
            } else {
                self.keys.encode(i, &mut self.key);
            }
            if self.key.first() == Some(&b'"') {
                out.extend_from_slice(&self.key);
            } else {
                let _ = serde_json::to_writer(&mut *out, String::from_utf8_lossy(&self.key).as_ref());
            }
            out.push(b':');
            if self.values.is_null(i) {
                out.extend_from_slice(b"null");
            } else {
                self.values.encode(i, out);
            }
        }
        out.push(b'}');
    }
}

/// Dates and timestamps as JSON strings in the CSV output's format.
struct TemporalEncoder<'a>(&'a dyn Array);

impl Encoder for TemporalEncoder<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.push(b'"');
        match self.0.data_type() {
            DataType::Timestamp(unit, tz) => crate::csvout::write_timestamp_at(
                out,
                self.0,
                idx,
                *unit,
                crate::csvout::timestamp_offset(tz.as_deref()),
            ),
            DataType::Date64 => crate::csvout::write_date64(out, self.0.as_primitive::<Date64Type>().value(idx)),
            _ => crate::csvout::write_date(out, self.0.as_primitive::<Date32Type>().value(idx) as i64),
        }
        out.push(b'"');
    }
}

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
        field: &'a FieldRef,
        array: &'a dyn Array,
        options: &'a EncoderOptions,
    ) -> std::result::Result<Option<NullableEncoder<'a>>, ArrowError> {
        let marked = |f: &FieldRef| f.metadata().contains_key(JSON_TEXT);
        Ok(match array.data_type() {
            t @ (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
                match field.metadata().get(JSON_TEXT).map(String::as_str) {
                    Some(kind @ ("number" | "value")) => {
                        let number = kind == "number";
                        let encoder: Box<dyn Encoder + 'a> = match t {
                            DataType::Utf8 => Box::new(RawJson(array.as_string::<i32>(), number)),
                            DataType::LargeUtf8 => Box::new(RawJson(array.as_string::<i64>(), number)),
                            _ => Box::new(RawJson(array.as_string_view(), number)),
                        };
                        Some(NullableEncoder::new(encoder, array.nulls().cloned()))
                    }
                    _ => None,
                }
            }
            DataType::List(item) if marked(item) => Some(marked_items(item, array.as_list::<i32>(), options)?),
            DataType::LargeList(item) if marked(item) => Some(marked_items(item, array.as_list::<i64>(), options)?),
            DataType::FixedSizeList(item, _) if marked(item) => {
                Some(marked_items(item, array.as_fixed_size_list(), options)?)
            }
            DataType::Float32 | DataType::Float64 => {
                Some(NullableEncoder::new(Box::new(FloatEncoder(array)), array.nulls().cloned()))
            }
            DataType::Map(entries, _) => match entries.data_type() {
                DataType::Struct(kv) if kv.len() == 2 => {
                    let map = array.as_map();
                    let keys = arrow::json::writer::make_encoder(&kv[0], map.keys().as_ref(), options)?;
                    let values = arrow::json::writer::make_encoder(&kv[1], map.values().as_ref(), options)?;
                    let encoder = MapObject { offsets: map.value_offsets(), keys, values, key: Vec::new() };
                    Some(NullableEncoder::new(Box::new(encoder), map.nulls().cloned()))
                }
                _ => None,
            },
            DataType::Timestamp(..) | DataType::Date32 | DataType::Date64 => {
                Some(NullableEncoder::new(Box::new(TemporalEncoder(array)), array.nulls().cloned()))
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
