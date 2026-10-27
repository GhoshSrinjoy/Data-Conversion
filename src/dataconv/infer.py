"""Lossless type inference rules (the Python twin of the native ``types.rs``).

A column only gets a typed representation when every value converts without
losing information:

* integers must be canonical ``-?(0|[1-9][0-9]*)``: ``007`` (zip codes, IDs) and
  ``+4915...`` (phone numbers) stay text; values beyond int64 become exact decimals;
* floats must round-trip: up to 15 significant digits always do, 16-17 digits are
  checked one by one, anything longer becomes an exact decimal or text;
* timestamps keep their precision and UTC offset (mixed offsets stay text);
* placeholders such as ``NA`` or ``null`` mean "missing" only in typed columns.

Both engines apply the same rules, so a file gets the same schema no matter which
engine converts it.
"""

from __future__ import annotations

import datetime as _dt
import math
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, List, Optional, Sequence, Tuple

NULL_TOKENS = frozenset(["", "NA", "N/A", "n/a", "na", "NULL", "null", "Null", "None", "none", "nil", "NIL", "#N/A", "-"])
NAN_TOKENS = frozenset(["NaN", "nan", "NAN"])

_INT = re.compile(r"-?(?:0|[1-9][0-9]*)")
_INT_LOOSE = re.compile(r"[+-]?[0-9]+")
_FLOAT = re.compile(r"(-?)((?:0|[1-9][0-9]*)?)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?")
_FLOAT_LOOSE = re.compile(r"([+-]?)([0-9]*)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?")
_SPECIAL = re.compile(r"-?(?:inf|infinity|nan)", re.IGNORECASE)
_BOOL = {"true": True, "True": True, "TRUE": True, "false": False, "False": False, "FALSE": False}
_DATE = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})")
_TS = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})[T ]([0-9]{2}):([0-9]{2})"
    r"(?::([0-9]{2})(?:\.([0-9]{1,9}))?)?"
    r"(Z|[+-][0-9]{2}(?::?[0-9]{2})?)?"
)
INT64_MIN, INT64_MAX = -(1 << 63), (1 << 63) - 1
_EPOCH = _dt.date(1970, 1, 1).toordinal()


def is_null(v: str, extra: Sequence[str] = ()) -> bool:
    return v in NULL_TOKENS or (bool(extra) and v in extra)


def is_nan(v: str) -> bool:
    return v in NAN_TOKENS


def parse_int(v: str, loose: bool = False) -> Tuple[str, Optional[int], int]:
    """('ok', value, digits) | ('overflow', None, significant digits) | ('no', None, 0)."""
    if not (_INT_LOOSE if loose else _INT).fullmatch(v) or (not loose and v == "-0"):
        return "no", None, 0
    value = int(v)
    if INT64_MIN <= value <= INT64_MAX:
        return "ok", value, sum(c.isdigit() for c in v)
    return "overflow", None, len(v.lstrip("+-").lstrip("0")) or 1


@dataclass
class FloatInfo:
    value: float
    lossless: bool
    plain: bool
    int_digits: int
    frac_digits: int


def _faithful(value: float, sig: str, sci_exp: int) -> bool:
    text = f"{abs(value):.{len(sig) - 1}e}"
    mantissa, _, exponent = text.partition("e")
    digits = mantissa.replace(".", "").rstrip("0")
    return int(exponent) == sci_exp and digits == sig


def parse_float(v: str, loose: bool = False) -> Optional[FloatInfo]:
    if _SPECIAL.fullmatch(v):
        return FloatInfo(float(v), True, False, 0, 0)
    m = (_FLOAT_LOOSE if loose else _FLOAT).fullmatch(v)
    if not m:
        return None
    _, int_part, frac_part, exp = m.groups()
    frac_part = frac_part or ""
    if not int_part and not frac_part:
        return None
    if not loose and len(int_part) > 1 and int_part[0] == "0":
        return None
    try:
        value = float(v)
    except ValueError:
        return None
    digits = int_part + frac_part
    stripped = digits.lstrip("0")
    lead = len(digits) - len(stripped)
    sig = stripped.rstrip("0")
    if loose or not sig:
        lossless = math.isfinite(value) or loose
    elif not math.isfinite(value) or value == 0.0:
        lossless = False
    elif len(sig) >= 18:
        lossless = False
    elif len(sig) <= 15 and abs(value) >= 2.2250738585072014e-308:
        lossless = True
    else:  # 16-17 digits, or a subnormal (which keeps fewer digits): check exactly
        sci_exp = len(int_part) - 1 - lead + int(exp or 0)
        lossless = _faithful(value, sig, sci_exp)
    return FloatInfo(
        value,
        lossless,
        exp is None,
        max(1, len(int_part.lstrip("0"))),
        len(frac_part),
    )


def _days_in_month(y: int, m: int) -> int:
    if m == 2:
        return 29 if (y % 4 == 0 and y % 100 != 0) or y % 400 == 0 else 28
    return 30 if m in (4, 6, 9, 11) else 31


def days_from_civil(y: int, m: int, d: int) -> int:
    y = y - 1 if m <= 2 else y
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    mp = (m + 9) % 12
    doy = (153 * mp + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def parse_date(v: str) -> Optional[int]:
    """'YYYY-MM-DD' -> days since 1970-01-01."""
    m = _DATE.fullmatch(v)
    if not m:
        return None
    y, mo, d = (int(x) for x in m.groups())
    if not 1 <= mo <= 12 or not 1 <= d <= _days_in_month(y, mo):
        return None
    return days_from_civil(y, mo, d)


@dataclass
class Timestamp:
    local_seconds: int
    nanos: int
    frac_digits: int
    #: None for naive values, else the UTC offset in minutes.
    offset: Optional[int]

    def utc_seconds(self) -> int:
        return self.local_seconds - (self.offset or 0) * 60

    def to_unit(self, unit: str) -> int:
        per = {"s": 1, "ms": 1000, "us": 1_000_000, "ns": 1_000_000_000}[unit]
        return self.utc_seconds() * per + self.nanos // (1_000_000_000 // per)


def parse_timestamp(v: str) -> Optional[Timestamp]:
    m = _TS.fullmatch(v)
    if not m:
        return None
    y, mo, d, hh, mi, ss, frac, tz = m.groups()
    days = parse_date(f"{y}-{mo}-{d}")
    if days is None:
        return None
    h, mn, s = int(hh), int(mi), int(ss or 0)
    if h > 23 or mn > 59 or s > 59:
        return None
    nanos = int((frac or "").ljust(9, "0")) if frac else 0
    offset: Optional[int] = None
    if tz:
        if tz == "Z":
            offset = 0
        else:
            sign = -1 if tz[0] == "-" else 1
            body = tz[1:].replace(":", "")
            oh, om = int(body[:2]), int(body[2:4] or 0)
            if oh > 23 or om > 59:
                return None
            offset = sign * (oh * 60 + om)
    return Timestamp(days * 86400 + h * 3600 + mn * 60 + s, nanos, len(frac or ""), offset)


def tz_name(offset: int) -> str:
    if offset == 0:
        return "UTC"
    sign = "-" if offset < 0 else "+"
    return f"{sign}{abs(offset) // 60:02d}:{abs(offset) % 60:02d}"


def unit_for(frac_digits: int) -> str:
    return "s" if frac_digits == 0 else "ms" if frac_digits <= 3 else "us" if frac_digits <= 6 else "ns"


# ---------------------------------------------------------------------------------
# Logical types
# ---------------------------------------------------------------------------------


@dataclass(frozen=True)
class LogicalType:
    kind: str  # string bool int64 float64 decimal date timestamp
    precision: int = 0
    scale: int = 0
    unit: str = ""
    tz: Optional[str] = None

    @property
    def label(self) -> str:
        if self.kind == "decimal":
            return f"decimal({self.precision},{self.scale})"
        if self.kind == "timestamp":
            return f"timestamp[{self.unit}]" if self.tz is None else f"timestamp[{self.unit},{self.tz}]"
        return self.kind

    def __str__(self) -> str:
        return self.label

    def arrow(self) -> Any:
        import pyarrow as pa

        return {
            "string": pa.string,
            "bool": pa.bool_,
            "int64": pa.int64,
            "float64": pa.float64,
            "date": pa.date32,
        }.get(self.kind, lambda: None)() or (
            pa.decimal128(self.precision, self.scale) if self.kind == "decimal" else pa.timestamp(self.unit, self.tz)
        )


STRING = LogicalType("string")


def parse_type(text: str) -> LogicalType:
    t = text.strip().lower().replace(" ", "")
    simple = {
        "string": "string", "str": "string", "text": "string", "utf8": "string",
        "bool": "bool", "boolean": "bool",
        "int": "int64", "int64": "int64", "integer": "int64", "long": "int64",
        "float": "float64", "float64": "float64", "double": "float64", "number": "float64",
        "date": "date", "date32": "date",
    }  # fmt: skip
    if t in simple:
        return LogicalType(simple[t])
    if t in ("timestamp", "datetime"):
        return LogicalType("timestamp", unit="us")
    m = re.fullmatch(r"decimal\((\d+)(?:,(\d+))?\)", t)
    if m:
        p, s = int(m.group(1)), int(m.group(2) or 0)
        if 0 < p <= 38 and s <= p:
            return LogicalType("decimal", p, s)
    m = re.fullmatch(r"timestamp\[(s|ms|us|ns)(?:,([^\]]+))?\]", t)
    if m:
        tz = m.group(2)
        if tz is not None:
            tz = "UTC" if tz in ("utc", "z") else tz.upper()
        return LogicalType("timestamp", unit=m.group(1), tz=tz)
    raise ValueError(f"invalid type {text!r}")


class ColumnStats:
    """Mergeable per-column summary deciding the column's type."""

    __slots__ = (
        "float_lossy",
        "float_plain",
        "int_overflow",
        "max_frac_digits",
        "max_int_digits",
        "max_intpart_digits",
        "maybe_bool",
        "maybe_date",
        "maybe_float",
        "maybe_int",
        "maybe_ts",
        "note",
        "nulls",
        "ts_frac_digits",
        "ts_seen",
        "ts_tz",
        "ts_tz_mixed",
        "values",
    )  # fmt: skip

    def __init__(self) -> None:
        self.values = 0
        self.nulls = 0
        self.maybe_bool = self.maybe_int = self.maybe_float = self.maybe_date = self.maybe_ts = True
        self.int_overflow = False
        self.max_int_digits = 0
        self.float_lossy = False
        self.float_plain = True
        self.max_frac_digits = 0
        self.max_intpart_digits = 0
        self.ts_frac_digits = 0
        self.ts_tz: Optional[int] = None
        self.ts_seen = False
        self.ts_tz_mixed = False
        self.note: Optional[str] = None

    def _typed(self) -> bool:
        return self.maybe_bool or self.maybe_int or self.maybe_float or self.maybe_date or self.maybe_ts

    def observe(self, v: str, mode: str = "infer", extra_nulls: Sequence[str] = ()) -> None:
        if is_null(v, extra_nulls) or is_nan(v):
            self.nulls += 1
            return
        self.values += 1
        if mode == "string" or not self._typed():
            self.maybe_bool = self.maybe_int = self.maybe_float = self.maybe_date = self.maybe_ts = False
            return
        loose = mode == "loose"
        if self.maybe_bool and v not in _BOOL:
            self.maybe_bool = False
        if self.maybe_int:
            status, _, digits = parse_int(v, loose)
            if status == "ok":
                self.max_int_digits = max(self.max_int_digits, digits)
                self.max_intpart_digits = max(self.max_intpart_digits, digits)
                if digits <= 15:
                    self.maybe_date = self.maybe_ts = False
                    return
            elif status == "overflow":
                self.int_overflow = True
                self.max_int_digits = max(self.max_int_digits, digits)
            else:
                if self.note is None and not loose and parse_int(v, True)[0] != "no":
                    self.note = f"leading zeros or '+' signs would be lost (e.g. '{v[:40]}')"
                self.maybe_int = False
        if self.maybe_float:
            info = parse_float(v, loose)
            if info is None:
                self.maybe_float = False
            else:
                self.float_lossy |= not info.lossless
                self.float_plain &= info.plain
                self.max_frac_digits = max(self.max_frac_digits, info.frac_digits)
                self.max_intpart_digits = max(self.max_intpart_digits, info.int_digits)
        if self.maybe_date and parse_date(v) is None:
            self.maybe_date = False
        if self.maybe_ts:
            ts = parse_timestamp(v)
            if ts is None:
                self.maybe_ts = False
            else:
                self.ts_frac_digits = max(self.ts_frac_digits, ts.frac_digits)
                key = -100000 if ts.offset is None else ts.offset
                if not self.ts_seen:
                    self.ts_seen, self.ts_tz = True, key
                elif self.ts_tz != key:
                    self.ts_tz_mixed = True

    def observe_many(self, values: Iterable[str], mode: str = "infer", extra_nulls: Sequence[str] = ()) -> None:
        for v in values:
            self.observe(v, mode, extra_nulls)

    def merge(self, o: ColumnStats) -> None:
        self.values += o.values
        self.nulls += o.nulls
        self.maybe_bool &= o.maybe_bool
        self.maybe_int &= o.maybe_int
        self.maybe_float &= o.maybe_float
        self.maybe_date &= o.maybe_date
        self.maybe_ts &= o.maybe_ts
        self.int_overflow |= o.int_overflow
        self.max_int_digits = max(self.max_int_digits, o.max_int_digits)
        self.float_lossy |= o.float_lossy
        self.float_plain &= o.float_plain
        self.max_frac_digits = max(self.max_frac_digits, o.max_frac_digits)
        self.max_intpart_digits = max(self.max_intpart_digits, o.max_intpart_digits)
        self.ts_frac_digits = max(self.ts_frac_digits, o.ts_frac_digits)
        self.ts_tz_mixed |= o.ts_tz_mixed
        if o.ts_seen:
            if not self.ts_seen:
                self.ts_seen, self.ts_tz = True, o.ts_tz
            elif self.ts_tz != o.ts_tz:
                self.ts_tz_mixed = True
        if self.note is None:
            self.note = o.note

    def decide(self) -> LogicalType:
        if self.values == 0:
            return STRING
        if self.maybe_bool:
            return LogicalType("bool")
        if self.maybe_int:
            if not self.int_overflow:
                return LogicalType("int64")
            if self.max_int_digits <= 38:
                return LogicalType("decimal", max(1, self.max_int_digits), 0)
            return STRING
        if self.maybe_float:
            if not self.float_lossy:
                return LogicalType("float64")
            precision = self.max_intpart_digits + self.max_frac_digits
            if self.float_plain and precision <= 38:
                return LogicalType("decimal", max(1, precision), self.max_frac_digits)
            return STRING
        if self.maybe_date:
            return LogicalType("date")
        if self.maybe_ts and not self.ts_tz_mixed and self.ts_seen:
            tz = None if self.ts_tz == -100000 else tz_name(self.ts_tz or 0)
            return LogicalType("timestamp", unit=unit_for(self.ts_frac_digits), tz=tz)
        return STRING

    def explain(self, name: str) -> Optional[str]:
        ty = self.decide()
        if ty.kind == "decimal" and self.maybe_int:
            return f"column '{name}': integers exceed int64, stored exactly as {ty.label}"
        if ty.kind == "decimal":
            return f"column '{name}': more digits than float64 holds, stored exactly as {ty.label}"
        if ty.kind == "string" and self.ts_tz_mixed and self.maybe_ts:
            return f"column '{name}' kept as text: it mixes UTC offsets"
        if ty.kind == "string" and self.note:
            return f"column '{name}' kept as text: {self.note}"
        return None


def infer_types(columns: Sequence[Sequence[str]], mode: str = "infer", extra_nulls: Sequence[str] = ()) -> List[LogicalType]:
    out = []
    for col in columns:
        s = ColumnStats()
        s.observe_many(col, mode, extra_nulls)
        out.append(s.decide())
    return out


# ---------------------------------------------------------------------------------
# Value conversion (pure-Python engine)
# ---------------------------------------------------------------------------------


class Violation(ValueError):
    def __init__(self, column: int, value: str) -> None:
        super().__init__(f"value {value!r} does not fit column {column}")
        self.column = column
        self.value = value


def _tzinfo(tz: Optional[str]) -> Optional[_dt.tzinfo]:
    if tz is None:
        return None
    if tz == "UTC":
        return _dt.timezone.utc
    sign = -1 if tz[0] == "-" else 1
    return _dt.timezone(sign * _dt.timedelta(hours=int(tz[1:3]), minutes=int(tz[4:6])))


def converter(ty: LogicalType, mode: str = "infer", extra_nulls: Sequence[str] = ()):
    """A function text -> Python value for `ty` (raises Violation-style ValueError)."""
    loose = mode == "loose"
    kind = ty.kind
    if kind == "string":
        return lambda v, missing=False: None if missing else v

    def nullish(v: str) -> bool:
        return is_null(v, extra_nulls) or (kind != "float64" and is_nan(v))

    if kind == "bool":
        def conv(v, missing=False):
            if missing or nullish(v):
                return None
            return _BOOL[v]
    elif kind == "int64":
        def conv(v, missing=False):
            if missing or nullish(v):
                return None
            status, value, _ = parse_int(v, loose)
            if status != "ok":
                raise ValueError(v)
            return value
    elif kind == "float64":
        def conv(v, missing=False):
            if missing or is_null(v, extra_nulls):
                return None
            if is_nan(v):
                return math.nan
            info = parse_float(v, loose)
            if info is None or not info.lossless:
                raise ValueError(v)
            return info.value
    elif kind == "decimal":
        limit = 10**ty.precision
        quant = Decimal(1).scaleb(-ty.scale)

        def conv(v, missing=False):
            if missing or nullish(v):
                return None
            # canonical only: leading zeros and the sign of -0 would be lost
            if not re.fullmatch(r"-?(?:0|[1-9][0-9]*)?(?:\.[0-9]*)?", v) or v in ("", "-", ".", "-."):
                raise ValueError(v)
            frac = v.partition(".")[2]
            if len(frac) > ty.scale:
                raise ValueError(v)
            d = Decimal(v).quantize(quant)
            if abs(d.scaleb(ty.scale)) >= limit or (v.startswith("-") and d == 0):
                raise ValueError(v)
            return d
    elif kind == "date":
        def conv(v, missing=False):
            if missing or nullish(v):
                return None
            days = parse_date(v)
            if days is None:
                raise ValueError(v)
            return _dt.date.fromordinal(days + _EPOCH)
    else:
        tzinfo = _tzinfo(ty.tz)
        expect = None if ty.tz is None else (0 if ty.tz == "UTC" else int(tzinfo.utcoffset(None).total_seconds() // 60))  # type: ignore[union-attr]
        max_frac = {"s": 0, "ms": 3, "us": 6, "ns": 9}[ty.unit]

        def conv(v, missing=False):
            if missing or nullish(v):
                return None
            ts = parse_timestamp(v)
            if ts is None or ts.offset != expect or ts.frac_digits > max_frac:
                raise ValueError(v)
            base = _dt.datetime(1970, 1, 1) + _dt.timedelta(seconds=ts.local_seconds, microseconds=ts.nanos // 1000)
            return base.replace(tzinfo=tzinfo) if tzinfo else base

    return conv
