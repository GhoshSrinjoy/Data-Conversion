//! Lossless type inference.
//!
//! A column only gets a typed representation when every value converts without
//! losing information:
//!
//! * integers must be canonical: `-?(0|[1-9][0-9]*)`.  `007` (zip codes, IDs) and
//!   `+4915...` (phone numbers) stay text, and values beyond int64 become
//!   `decimal(p, 0)` instead of being rounded to a float;
//! * floats must round-trip: up to 15 significant digits always do; 16 and 17 digits
//!   are checked one by one; anything longer becomes an exact `decimal(p, s)` or text;
//! * timestamps keep their unit (s/ms/us/ns from the fractional digits) and their UTC
//!   offset (a column mixing offsets stays text so no local time is lost);
//! * text placeholders (`NA`, `null`, `-`, ...) mean "missing" only in columns that are
//!   otherwise typed; in text columns they are kept verbatim.
//!
//! The same rules are implemented by the Python engines (see `dataconv/infer.py`).

use serde::{Deserialize, Serialize};

pub const NULL_TOKENS: &[&[u8]] =
    &[b"", b"NA", b"N/A", b"n/a", b"na", b"NULL", b"null", b"Null", b"None", b"none", b"nil", b"NIL", b"#N/A", b"-"];
pub const NAN_TOKENS: &[&[u8]] = &[b"NaN", b"nan", b"NAN"];

#[inline]
pub fn is_null_token(v: &[u8], extra: &[Vec<u8>]) -> bool {
    if v.is_empty() {
        return true;
    }
    if v.len() > 4 && extra.is_empty() {
        return false;
    }
    NULL_TOKENS.contains(&v) || extra.iter().any(|t| t.as_slice() == v)
}

#[inline]
pub fn is_nan_token(v: &[u8]) -> bool {
    v.len() == 3 && NAN_TOKENS.contains(&v)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TypesMode {
    /// Lossless inference (default).
    #[default]
    Infer,
    /// Every column is text.
    String,
    /// Permissive: leading zeros and '+' become numbers, floats may round.
    Loose,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Unit {
    S,
    Ms,
    Us,
    Ns,
}

impl Unit {
    pub fn from_frac_digits(d: u8) -> Unit {
        match d {
            0 => Unit::S,
            1..=3 => Unit::Ms,
            4..=6 => Unit::Us,
            _ => Unit::Ns,
        }
    }

    pub fn per_second(self) -> i64 {
        match self {
            Unit::S => 1,
            Unit::Ms => 1_000,
            Unit::Us => 1_000_000,
            Unit::Ns => 1_000_000_000,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Unit::S => "s",
            Unit::Ms => "ms",
            Unit::Us => "us",
            Unit::Ns => "ns",
        }
    }

    pub fn arrow(self) -> arrow::datatypes::TimeUnit {
        use arrow::datatypes::TimeUnit;
        match self {
            Unit::S => TimeUnit::Second,
            Unit::Ms => TimeUnit::Millisecond,
            Unit::Us => TimeUnit::Microsecond,
            Unit::Ns => TimeUnit::Nanosecond,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum LogicalType {
    String,
    Bool,
    Int64,
    Float64,
    Decimal { precision: u8, scale: u8 },
    Date,
    Timestamp { unit: Unit, tz: Option<String> },
}

impl LogicalType {
    pub fn arrow(&self) -> arrow::datatypes::DataType {
        use arrow::datatypes::DataType;
        match self {
            LogicalType::String => DataType::Utf8,
            LogicalType::Bool => DataType::Boolean,
            LogicalType::Int64 => DataType::Int64,
            LogicalType::Float64 => DataType::Float64,
            LogicalType::Decimal { precision, scale } => DataType::Decimal128(*precision, *scale as i8),
            LogicalType::Date => DataType::Date32,
            LogicalType::Timestamp { unit, tz } => DataType::Timestamp(unit.arrow(), tz.as_deref().map(Into::into)),
        }
    }

    /// Short human-readable name (also the syntax accepted by schema overrides).
    pub fn label(&self) -> String {
        match self {
            LogicalType::String => "string".into(),
            LogicalType::Bool => "bool".into(),
            LogicalType::Int64 => "int64".into(),
            LogicalType::Float64 => "float64".into(),
            LogicalType::Decimal { precision, scale } => format!("decimal({precision},{scale})"),
            LogicalType::Date => "date".into(),
            LogicalType::Timestamp { unit, tz: None } => format!("timestamp[{}]", unit.name()),
            LogicalType::Timestamp { unit, tz: Some(tz) } => format!("timestamp[{},{}]", unit.name(), tz),
        }
    }

    /// Parse the override syntax: string, int64, float64, bool, date,
    /// decimal(p,s), timestamp[unit] or timestamp[unit,tz].
    pub fn parse(text: &str) -> Option<LogicalType> {
        let t = text.trim().to_ascii_lowercase();
        Some(match t.as_str() {
            "string" | "str" | "text" | "utf8" => LogicalType::String,
            "bool" | "boolean" => LogicalType::Bool,
            "int" | "int64" | "integer" | "long" => LogicalType::Int64,
            "float" | "float64" | "double" | "number" => LogicalType::Float64,
            "date" | "date32" => LogicalType::Date,
            "timestamp" | "datetime" => LogicalType::Timestamp { unit: Unit::Us, tz: None },
            _ => {
                if let Some(inner) = t.strip_prefix("decimal(").and_then(|r| r.strip_suffix(')')) {
                    let mut parts = inner.split(',');
                    let p: u8 = parts.next()?.trim().parse().ok()?;
                    let s: u8 = parts.next().map(|s| s.trim().parse().ok()).unwrap_or(Some(0))?;
                    if p == 0 || p > 38 || s > p {
                        return None;
                    }
                    LogicalType::Decimal { precision: p, scale: s }
                } else if let Some(inner) = t.strip_prefix("timestamp[").and_then(|r| r.strip_suffix(']')) {
                    let mut parts = inner.splitn(2, ',');
                    let unit = match parts.next()?.trim() {
                        "s" => Unit::S,
                        "ms" => Unit::Ms,
                        "us" => Unit::Us,
                        "ns" => Unit::Ns,
                        _ => return None,
                    };
                    let tz = parts.next().map(|z| {
                        let z = z.trim();
                        if z.eq_ignore_ascii_case("utc") || z == "z" {
                            "UTC".to_string()
                        } else {
                            z.to_uppercase()
                        }
                    });
                    LogicalType::Timestamp { unit, tz }
                } else {
                    return None;
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------------
// Value classifiers
// ---------------------------------------------------------------------------------

pub enum IntParse {
    Ok(i64),
    /// Canonical integer that does not fit in int64; carries its digit count.
    Overflow(u32),
    No,
}

#[inline]
pub fn parse_int(v: &[u8], loose: bool) -> IntParse {
    let (neg, digits) = match v.first() {
        Some(b'-') => (true, &v[1..]),
        Some(b'+') if loose => (false, &v[1..]),
        _ => (false, v),
    };
    if digits.is_empty() || (!loose && digits.len() > 1 && digits[0] == b'0') {
        return IntParse::No;
    }
    let mut acc: u64 = 0;
    let mut overflow = false;
    for &b in digits {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return IntParse::No;
        }
        if !overflow {
            match acc.checked_mul(10).and_then(|x| x.checked_add(d as u64)) {
                Some(x) => acc = x,
                None => overflow = true,
            }
        }
    }
    let limit = if neg { 1u64 << 63 } else { (1u64 << 63) - 1 };
    if overflow || acc > limit {
        let significant = digits.iter().skip_while(|&&b| b == b'0').count().max(1);
        return IntParse::Overflow(significant as u32);
    }
    IntParse::Ok(if neg { (acc as i64).wrapping_neg() } else { acc as i64 })
}

#[derive(Clone, Copy, Debug)]
pub struct FloatInfo {
    pub value: f64,
    /// Representable as f64 without changing its decimal value.
    pub lossless: bool,
    /// Plain decimal notation (no exponent, not inf/nan).
    pub plain: bool,
    pub int_digits: u32,
    pub frac_digits: u32,
}

#[inline]
fn is_special_float(v: &[u8]) -> Option<f64> {
    let body = v.strip_prefix(b"-").unwrap_or(v);
    let neg = body.len() != v.len();
    let value = if body.eq_ignore_ascii_case(b"inf") || body.eq_ignore_ascii_case(b"infinity") {
        f64::INFINITY
    } else if body.eq_ignore_ascii_case(b"nan") {
        f64::NAN
    } else {
        return None;
    };
    Some(if neg { -value } else { value })
}

/// Classify a float literal: `-?(0|[1-9][0-9]*)(\.[0-9]*)?([eE][+-]?[0-9]+)?`, `-?\.[0-9]+...`
/// or inf/infinity/nan.  Returns None when the text is not a float.
pub fn parse_float(v: &[u8], loose: bool) -> Option<FloatInfo> {
    if let Some(value) = is_special_float(v) {
        return Some(FloatInfo { value, lossless: true, plain: false, int_digits: 0, frac_digits: 0 });
    }
    let mut i = 0;
    let n = v.len();
    if i < n && (v[i] == b'-' || (loose && v[i] == b'+')) {
        i += 1;
    }
    let int_start = i;
    while i < n && v[i].is_ascii_digit() {
        i += 1;
    }
    let int_part = &v[int_start..i];
    if !loose && int_part.len() > 1 && int_part[0] == b'0' {
        return None;
    }
    let mut frac_part: &[u8] = &[];
    if i < n && v[i] == b'.' {
        i += 1;
        let fs = i;
        while i < n && v[i].is_ascii_digit() {
            i += 1;
        }
        frac_part = &v[fs..i];
    }
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    let mut exp: i64 = 0;
    let mut has_exp = false;
    if i < n && (v[i] == b'e' || v[i] == b'E') {
        has_exp = true;
        i += 1;
        let mut neg = false;
        if i < n && (v[i] == b'+' || v[i] == b'-') {
            neg = v[i] == b'-';
            i += 1;
        }
        let es = i;
        while i < n && v[i].is_ascii_digit() {
            if exp < 100_000 {
                exp = exp * 10 + (v[i] - b'0') as i64;
            }
            i += 1;
        }
        if i == es {
            return None;
        }
        if neg {
            exp = -exp;
        }
    }
    if i != n {
        return None;
    }
    let text = std::str::from_utf8(v).ok()?;
    let value: f64 = text.parse().ok()?;
    // Significant digits: the digit sequence without leading and trailing zeros.
    let total = int_part.len() + frac_part.len();
    let digit_at = |k: usize| if k < int_part.len() { int_part[k] } else { frac_part[k - int_part.len()] };
    let mut lead = 0;
    while lead < total && digit_at(lead) == b'0' {
        lead += 1;
    }
    let mut trail = 0;
    while trail < total - lead && digit_at(total - 1 - trail) == b'0' {
        trail += 1;
    }
    let sig_len = total - lead - trail;
    let lossless = if loose || sig_len == 0 {
        value.is_finite() || loose
    } else if !value.is_finite() || value == 0.0 {
        false
    } else if sig_len <= 15 {
        true
    } else if sig_len >= 18 {
        false
    } else {
        let sig: Vec<u8> = (lead..lead + sig_len).map(digit_at).collect();
        // Decimal exponent of the first significant digit.
        let sci_exp = int_part.len() as i64 - 1 - lead as i64 + exp;
        faithful(value, &sig, sci_exp)
    };
    Some(FloatInfo {
        value,
        lossless,
        plain: !has_exp,
        int_digits: int_part.iter().skip_while(|&&b| b == b'0').count().max(1) as u32,
        frac_digits: frac_part.len() as u32,
    })
}

/// Whether printing `value` with as many significant digits as `sig` reproduces the
/// same decimal number, i.e. the text is a faithful rendering of that double.
fn faithful(value: f64, sig: &[u8], sci_exp: i64) -> bool {
    let formatted = format!("{:.*e}", sig.len() - 1, value.abs());
    let (mantissa, exponent) = match formatted.split_once('e') {
        Some(parts) => parts,
        None => return false,
    };
    let exponent: i64 = match exponent.parse() {
        Ok(e) => e,
        Err(_) => return false,
    };
    let fdigits: Vec<u8> = mantissa.bytes().filter(|b| b.is_ascii_digit()).collect();
    let ftrim = fdigits.iter().rev().take_while(|&&b| b == b'0').count();
    exponent == sci_exp && &fdigits[..fdigits.len() - ftrim] == sig
}

#[inline]
pub fn parse_bool(v: &[u8]) -> Option<bool> {
    match v {
        b"true" | b"True" | b"TRUE" => Some(true),
        b"false" | b"False" | b"FALSE" => Some(false),
        _ => None,
    }
}

#[inline]
fn two(v: &[u8], i: usize) -> Option<u32> {
    let a = v.get(i)?.wrapping_sub(b'0');
    let b = v.get(i + 1)?.wrapping_sub(b'0');
    if a > 9 || b > 9 {
        return None;
    }
    Some(a as u32 * 10 + b as u32)
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(y) => 29,
        2 => 28,
        _ => 0,
    }
}

/// Days since 1970-01-01 for a proleptic Gregorian date.
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// `YYYY-MM-DD` → days since the epoch.
pub fn parse_date(v: &[u8]) -> Option<i32> {
    if v.len() != 10 || v[4] != b'-' || v[7] != b'-' {
        return None;
    }
    let y = two(v, 0)? as i64 * 100 + two(v, 2)? as i64;
    let m = two(v, 5)?;
    let d = two(v, 8)?;
    if m == 0 || m > 12 || d == 0 || d > days_in_month(y, m) {
        return None;
    }
    Some(days_from_civil(y, m, d) as i32)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tz {
    Naive,
    /// Offset east of UTC in minutes; `Z` and `+00:00` are both 0.
    Offset(i32),
}

#[derive(Clone, Copy, Debug)]
pub struct Timestamp {
    /// Seconds since the epoch of the local wall-clock time (offset not applied).
    pub local_seconds: i64,
    pub nanos: u32,
    pub frac_digits: u8,
    pub tz: Tz,
}

impl Timestamp {
    pub fn utc_seconds(&self) -> i64 {
        match self.tz {
            Tz::Naive => self.local_seconds,
            Tz::Offset(m) => self.local_seconds - m as i64 * 60,
        }
    }

    pub fn to_unit(&self, unit: Unit) -> Option<i64> {
        let secs = self.utc_seconds().checked_mul(unit.per_second())?;
        let sub = self.nanos as i64 / (1_000_000_000 / unit.per_second());
        secs.checked_add(sub)
    }
}

/// `YYYY-MM-DD[T ]HH:MM[:SS[.f{1,9}]][Z|±HH[:MM]|±HHMM]`
pub fn parse_timestamp(v: &[u8]) -> Option<Timestamp> {
    if v.len() < 16 || (v[10] != b'T' && v[10] != b' ') || v[13] != b':' {
        return None;
    }
    let days = parse_date(&v[..10])? as i64;
    let hh = two(v, 11)?;
    let mm = two(v, 14)?;
    let mut i = 16;
    let mut ss = 0u32;
    let mut nanos = 0u32;
    let mut frac_digits = 0u8;
    if i < v.len() && v[i] == b':' {
        ss = two(v, i + 1)?;
        i += 3;
        if i < v.len() && v[i] == b'.' {
            i += 1;
            let fs = i;
            while i < v.len() && v[i].is_ascii_digit() {
                i += 1;
            }
            let digits = i - fs;
            if digits == 0 || digits > 9 {
                return None;
            }
            let mut f = 0u32;
            for &b in &v[fs..i] {
                f = f * 10 + (b - b'0') as u32;
            }
            nanos = f * 10u32.pow(9 - digits as u32);
            frac_digits = digits as u8;
        }
    }
    if hh > 23 || mm > 59 || ss > 59 {
        return None;
    }
    let tz = if i == v.len() {
        Tz::Naive
    } else if v[i] == b'Z' && i + 1 == v.len() {
        Tz::Offset(0)
    } else if v[i] == b'+' || v[i] == b'-' {
        let sign = if v[i] == b'-' { -1 } else { 1 };
        let rest = &v[i + 1..];
        let (oh, om) = match rest.len() {
            2 => (two(rest, 0)?, 0),
            4 => (two(rest, 0)?, two(rest, 2)?),
            5 if rest[2] == b':' => (two(rest, 0)?, two(rest, 3)?),
            _ => return None,
        };
        if oh > 23 || om > 59 {
            return None;
        }
        Tz::Offset(sign * (oh as i32 * 60 + om as i32))
    } else {
        return None;
    };
    Some(Timestamp {
        local_seconds: days * 86_400 + hh as i64 * 3600 + mm as i64 * 60 + ss as i64,
        nanos,
        frac_digits,
        tz,
    })
}

pub fn tz_name(offset_minutes: i32) -> String {
    if offset_minutes == 0 {
        return "UTC".into();
    }
    let sign = if offset_minutes < 0 { '-' } else { '+' };
    let m = offset_minutes.abs();
    format!("{sign}{:02}:{:02}", m / 60, m % 60)
}

/// Parse a plain decimal (`-?digits[.digits]`) into an i128 scaled by 10^scale.
pub fn parse_decimal(v: &[u8], scale: u8) -> Option<i128> {
    let (neg, body) = match v.first() {
        Some(b'-') => (true, &v[1..]),
        _ => (false, v),
    };
    let (int_part, frac_part) = match memchr::memchr(b'.', body) {
        Some(p) => (&body[..p], &body[p + 1..]),
        None => (body, &body[body.len()..]),
    };
    if (int_part.is_empty() && frac_part.is_empty()) || frac_part.len() > scale as usize {
        return None;
    }
    let mut acc: i128 = 0;
    for &b in int_part.iter().chain(frac_part.iter()) {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_add(d as i128)?;
    }
    for _ in frac_part.len()..scale as usize {
        acc = acc.checked_mul(10)?;
    }
    Some(if neg { -acc } else { acc })
}

// ---------------------------------------------------------------------------------
// Column statistics: a mergeable summary that decides each column's type.
// ---------------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct ColumnStats {
    pub values: u64,
    pub nulls: u64,
    pub maybe_bool: bool,
    pub maybe_int: bool,
    pub maybe_float: bool,
    pub maybe_date: bool,
    pub maybe_ts: bool,
    pub int_overflow: bool,
    pub max_int_digits: u32,
    pub float_lossy: bool,
    pub float_plain: bool,
    pub max_frac_digits: u32,
    pub max_intpart_digits: u32,
    pub ts_frac_digits: u8,
    pub ts_tz: Option<Tz>,
    pub ts_tz_mixed: bool,
    pub max_len: u32,
    /// Why a numeric-looking column cannot be numeric (first example), for reports.
    pub note: Option<String>,
}

impl Default for ColumnStats {
    fn default() -> Self {
        ColumnStats {
            values: 0,
            nulls: 0,
            maybe_bool: true,
            maybe_int: true,
            maybe_float: true,
            maybe_date: true,
            maybe_ts: true,
            int_overflow: false,
            max_int_digits: 0,
            float_lossy: false,
            float_plain: true,
            max_frac_digits: 0,
            max_intpart_digits: 0,
            ts_frac_digits: 0,
            ts_tz: None,
            ts_tz_mixed: false,
            max_len: 0,
            note: None,
        }
    }
}

impl ColumnStats {
    #[inline]
    fn any_typed(&self) -> bool {
        self.maybe_bool || self.maybe_int || self.maybe_float || self.maybe_date || self.maybe_ts
    }

    /// Observe one value (already unescaped).  `quoted_empty` marks `""`, which is an
    /// empty string rather than a missing value.
    pub fn observe(&mut self, v: &[u8], mode: TypesMode, extra_nulls: &[Vec<u8>]) {
        self.max_len = self.max_len.max(v.len() as u32);
        if is_null_token(v, extra_nulls) || is_nan_token(v) {
            self.nulls += 1;
            return;
        }
        self.values += 1;
        if mode == TypesMode::String || !self.any_typed() {
            self.maybe_bool = false;
            self.maybe_int = false;
            self.maybe_float = false;
            self.maybe_date = false;
            self.maybe_ts = false;
            return;
        }
        let loose = mode == TypesMode::Loose;
        if self.maybe_bool && parse_bool(v).is_none() {
            self.maybe_bool = false;
        }
        if self.maybe_int {
            match parse_int(v, loose) {
                IntParse::Ok(_) => {
                    let digits = v.iter().filter(|b| b.is_ascii_digit()).count() as u32;
                    self.max_int_digits = self.max_int_digits.max(digits);
                    self.max_intpart_digits = self.max_intpart_digits.max(digits);
                    if digits <= 15 {
                        // Fast path: a short integer is also an exact, plain float, and
                        // never a date or timestamp.
                        self.maybe_date = false;
                        self.maybe_ts = false;
                        return;
                    }
                }
                IntParse::Overflow(d) => {
                    self.int_overflow = true;
                    self.max_int_digits = self.max_int_digits.max(d);
                }
                IntParse::No => {
                    if self.note.is_none() && !loose && !matches!(parse_int(v, true), IntParse::No) {
                        let shown = String::from_utf8_lossy(&v[..v.len().min(40)]).into_owned();
                        self.note = Some(format!("leading zeros or '+' signs would be lost (e.g. '{shown}')"));
                    }
                    self.maybe_int = false
                }
            }
        }
        if self.maybe_float {
            match parse_float(v, loose) {
                Some(info) => {
                    if !info.lossless {
                        self.float_lossy = true;
                    }
                    if !info.plain {
                        self.float_plain = false;
                    }
                    self.max_frac_digits = self.max_frac_digits.max(info.frac_digits);
                    self.max_intpart_digits = self.max_intpart_digits.max(info.int_digits);
                }
                None => self.maybe_float = false,
            }
        }
        if self.maybe_date && parse_date(v).is_none() {
            self.maybe_date = false;
        }
        if self.maybe_ts {
            match parse_timestamp(v) {
                Some(ts) => {
                    self.ts_frac_digits = self.ts_frac_digits.max(ts.frac_digits);
                    match self.ts_tz {
                        None => self.ts_tz = Some(ts.tz),
                        Some(prev) if prev != ts.tz => self.ts_tz_mixed = true,
                        _ => {}
                    }
                }
                None => self.maybe_ts = false,
            }
        }
    }

    pub fn merge(&mut self, o: &ColumnStats) {
        self.values += o.values;
        self.nulls += o.nulls;
        self.maybe_bool &= o.maybe_bool;
        self.maybe_int &= o.maybe_int;
        self.maybe_float &= o.maybe_float;
        self.maybe_date &= o.maybe_date;
        self.maybe_ts &= o.maybe_ts;
        self.int_overflow |= o.int_overflow;
        self.max_int_digits = self.max_int_digits.max(o.max_int_digits);
        self.float_lossy |= o.float_lossy;
        self.float_plain &= o.float_plain;
        self.max_frac_digits = self.max_frac_digits.max(o.max_frac_digits);
        self.max_intpart_digits = self.max_intpart_digits.max(o.max_intpart_digits);
        self.ts_frac_digits = self.ts_frac_digits.max(o.ts_frac_digits);
        self.ts_tz_mixed |= o.ts_tz_mixed;
        match (self.ts_tz, o.ts_tz) {
            (None, t) => self.ts_tz = t,
            (Some(a), Some(b)) if a != b => self.ts_tz_mixed = true,
            _ => {}
        }
        self.max_len = self.max_len.max(o.max_len);
        if self.note.is_none() {
            self.note = o.note.clone();
        }
    }

    pub fn decide(&self) -> LogicalType {
        if self.values == 0 {
            return LogicalType::String;
        }
        if self.maybe_bool {
            return LogicalType::Bool;
        }
        if self.maybe_int {
            if !self.int_overflow {
                return LogicalType::Int64;
            }
            if self.max_int_digits <= 38 {
                return LogicalType::Decimal { precision: self.max_int_digits.max(1) as u8, scale: 0 };
            }
            return LogicalType::String;
        }
        if self.maybe_float {
            if !self.float_lossy {
                return LogicalType::Float64;
            }
            let precision = self.max_intpart_digits + self.max_frac_digits;
            if self.float_plain && precision <= 38 {
                return LogicalType::Decimal { precision: precision.max(1) as u8, scale: self.max_frac_digits as u8 };
            }
            return LogicalType::String;
        }
        if self.maybe_date {
            return LogicalType::Date;
        }
        if self.maybe_ts && !self.ts_tz_mixed && self.ts_tz.is_some() {
            let tz = match self.ts_tz {
                Some(Tz::Offset(m)) => Some(tz_name(m)),
                _ => None,
            };
            return LogicalType::Timestamp { unit: Unit::from_frac_digits(self.ts_frac_digits), tz };
        }
        LogicalType::String
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn infer(values: &[&str]) -> LogicalType {
        let mut s = ColumnStats::default();
        for v in values {
            s.observe(v.as_bytes(), TypesMode::Infer, &[]);
        }
        s.decide()
    }

    #[test]
    fn integers_stay_lossless() {
        assert_eq!(infer(&["1", "-2", "30"]), LogicalType::Int64);
        assert_eq!(infer(&["007", "123"]), LogicalType::String);
        assert_eq!(infer(&["+4915112345678"]), LogicalType::String);
        assert_eq!(infer(&["0x10"]), LogicalType::String);
        assert_eq!(infer(&["18446744073709551615", "1"]), LogicalType::Decimal { precision: 20, scale: 0 });
        assert_eq!(infer(&["9223372036854775807", "-9223372036854775808"]), LogicalType::Int64);
        assert_eq!(infer(&["1", "", "NA", "3"]), LogicalType::Int64);
    }

    #[test]
    fn floats_stay_lossless() {
        assert_eq!(infer(&["0.1", "2.50", "1e-5", "-3"]), LogicalType::Float64);
        assert_eq!(infer(&["0.30000000000000004", "0.1"]), LogicalType::Float64);
        assert_eq!(infer(&["1.00000000000000000001"]), LogicalType::Decimal { precision: 21, scale: 20 });
        assert_eq!(infer(&["3.1415926535897932384"]), LogicalType::Decimal { precision: 20, scale: 19 });
        assert_eq!(infer(&["1.5", "nan", "inf"]), LogicalType::Float64);
        assert_eq!(infer(&["00.5"]), LogicalType::String);
        assert_eq!(infer(&["1e400"]), LogicalType::String);
        assert_eq!(infer(&["12345678901234567890123.5"]), LogicalType::Decimal { precision: 24, scale: 1 });
    }

    #[test]
    fn faithful_seventeen_digit_floats() {
        // 17-digit renderings of doubles are accepted ...
        for x in [0.1f64, 1.0 / 3.0, 2.0f64.sqrt(), 1e-300, 123456.789] {
            let text = format!("{:.16e}", x);
            assert!(parse_float(text.as_bytes(), false).unwrap().lossless, "{text}");
        }
        // ... but a 17-digit decimal that no double prints as is rejected: doubles near
        // 0.5 are 2^-53 apart, so 0.50000000000000006 rounds to 0.50000000000000011.
        assert!(!parse_float(b"0.50000000000000006", false).unwrap().lossless);
    }

    #[test]
    fn temporal() {
        assert_eq!(infer(&["2024-01-02", "1999-12-31"]), LogicalType::Date);
        assert_eq!(infer(&["2024-02-30"]), LogicalType::String);
        assert_eq!(infer(&["2024-02-29"]), LogicalType::Date);
        assert_eq!(infer(&["2023-02-29"]), LogicalType::String);
        assert_eq!(
            infer(&["2024-01-02 03:04:05", "2024-01-02T03:04:05.5"]),
            LogicalType::Timestamp { unit: Unit::Ms, tz: None }
        );
        assert_eq!(
            infer(&["2024-01-02T03:04:05Z", "2024-01-02 03:04:05.123456+00:00"]),
            LogicalType::Timestamp { unit: Unit::Us, tz: Some("UTC".into()) }
        );
        assert_eq!(
            infer(&["2024-01-02T03:04:05+05:30"]),
            LogicalType::Timestamp { unit: Unit::S, tz: Some("+05:30".into()) }
        );
        assert_eq!(infer(&["2024-01-02T03:04:05+05:30", "2024-01-02T03:04:05Z"]), LogicalType::String);
        assert_eq!(infer(&["2024-01-02T03:04:05", "2024-01-02T03:04:05Z"]), LogicalType::String);
        let ts = parse_timestamp(b"1970-01-01T01:00:00+01:00").unwrap();
        assert_eq!(ts.utc_seconds(), 0);
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2000, 3, 1), 11017);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
    }

    #[test]
    fn misc() {
        assert_eq!(infer(&["true", "False", "TRUE"]), LogicalType::Bool);
        assert_eq!(infer(&["1", "0"]), LogicalType::Int64);
        assert_eq!(infer(&["", "NA"]), LogicalType::String);
        assert_eq!(infer(&["NA", "Namibia"]), LogicalType::String);
        assert_eq!(parse_decimal(b"-12.5", 3), Some(-12500));
        assert_eq!(parse_decimal(b"12.345", 2), None);
        assert_eq!(LogicalType::parse("decimal(10, 2)"), Some(LogicalType::Decimal { precision: 10, scale: 2 }));
        assert_eq!(
            LogicalType::parse("timestamp[ms,utc]"),
            Some(LogicalType::Timestamp { unit: Unit::Ms, tz: Some("UTC".into()) })
        );
        let mut loose = ColumnStats::default();
        loose.observe(b"007", TypesMode::Loose, &[]);
        assert_eq!(loose.decide(), LogicalType::Int64);
    }
}
