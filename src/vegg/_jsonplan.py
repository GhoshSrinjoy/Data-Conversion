"""Column plans of JSON input: the Python twin of the native engine's JSON Lines
inference and decoding (``jsonl.rs``), shared by the pure and arrow engines.

What a field holds is folded value by value into a kind (objects key by key, keys in
first-seen order, arrays item by item); its plan then decides how values are decoded:

* integer literals that fit int64 are int64, longer ones decimal(p, 0) up to 38
  digits, and beyond that their exact text;
* other numbers are float64 when every value survives it, else decimal(p, s) in
  plain notation up to 38 digits, else their text (an exponent written ``e`` with a
  sign); the strings "NaN", "Infinity" and "-Infinity" count as floats in a number
  field;
* objects become structs (a key an object lacks is null) and arrays lists,
  recursively, every leaf by the rules above;
* a field mixing strings, numbers, booleans, objects and arrays holds each value's
  JSON text, as does an object that never has a field.

Text holding numbers or JSON is marked (``vegg.json`` = ``number`` / ``value``),
so that JSON output writes it as that JSON again.  A value that does not fit its plan
raises :class:`Misfit`; the engines then plan again from the whole file.
"""

from __future__ import annotations

import json
import math
import re
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

from . import infer


class JsonNumber(str):
    """A JSON number literal kept exactly as written: every non-integer and ``-0``."""

    __slots__ = ()


class JsonInt(JsonNumber):
    """An integer literal too long for any numeric type (kept as its text)."""

    __slots__ = ()


class JsonText(str):
    """The JSON text of a value held in a text column (a field mixing JSON types, a
    number kept as text, or an Arrow column marked ``vegg.json``): JSON output
    writes it as the JSON it holds, CSV output as its text."""

    __slots__ = ()


#: Arrow field metadata of text columns that hold JSON: "number" (numbers kept as
#: their text) or "value" (the JSON text of any value), as the native engine sets it
JSON_TEXT_KEY = "vegg.json"
#: The strings a number field may hold for the floats JSON cannot write
NAN_TEXT = {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}

_INT64_MIN, _INT64_MAX = -(1 << 63), (1 << 63) - 1
_EXACT = 10**15  # integers below this survive float64 (at most 15 significant digits)
_SURROGATE = re.compile("[\ud800-\udfff]")
_encode_str = json.encoder.encode_basestring  # json.dumps(s, ensure_ascii=False) of a str


def number_text(text: str) -> str:
    """A JSON number kept as text: as written, except that an exponent becomes a
    lower-case ``e`` with an explicit sign (``1E5`` -> ``1e+5``), like the native engine."""
    i = text.find("e")
    if i < 0:
        i = text.find("E")
        if i < 0:
            return str(text)
    exp = text[i + 1 :]
    return text[:i] + "e" + (exp if exp[:1] in ("+", "-") else "+" + exp)


def dump(v: Any) -> str:
    """Compact JSON text of a value read from JSON (numbers as :func:`number_text`)."""
    if v is None:
        return "null"
    if v is True:
        return "true"
    if v is False:
        return "false"
    t = type(v)
    if t is int:
        return str(v)
    if isinstance(v, JsonNumber):
        return number_text(v)
    if isinstance(v, JsonText):
        return str(v)
    if isinstance(v, str):
        return _encode_str(v)  # type: ignore[no-any-return]
    if t is dict:
        return "{" + ",".join(_encode_str(k) + ":" + dump(x) for k, x in v.items()) + "}"
    if t is list:
        return "[" + ",".join(dump(x) for x in v) + "]"
    raise TypeError(f"not a JSON value: {v!r}")


def parse_decimal(text: str, scale: int) -> Optional[int]:
    """The value of a plain decimal literal in units of ``10**-scale`` (None when it
    has more fraction digits, an exponent, leading zeros, or is ``-0``)."""
    neg = text[:1] == "-"
    body = text[1:] if neg else text
    whole, _, frac = body.partition(".")
    digits = whole + frac
    if not digits or len(frac) > scale or (len(whole) > 1 and whole[0] == "0"):
        return None
    if not (digits.isascii() and digits.isdigit()):
        return None
    acc = int(digits) * 10 ** (scale - len(frac))
    if acc >= 1 << 127 or (neg and acc == 0):
        return None
    return -acc if neg else acc


# ---------------------------------------------------------------------------------
# Kinds: what a field has looked like
# ---------------------------------------------------------------------------------


class _Tag:
    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return self.name


NULL = _Tag("null")
BOOL = _Tag("bool")
NANTEXT = _Tag("nan_text")  # only the strings "NaN", "Infinity" and "-Infinity"
STR = _Tag("str")
MIXED = _Tag("mixed")


class Num:
    """What the numbers of a field have looked like."""

    __slots__ = ("frac", "int_digits", "lossy", "nan", "no_decimal", "scale", "wide")

    def __init__(self, text: str) -> None:
        exp = "e" in text or "E" in text
        neg_zero = text[:1] == "-" and not text[1:].strip("0.")
        integer = not exp and "." not in text and not neg_zero
        # a fraction or an exponent, or -0 (an integer loses the sign)
        self.frac = not integer
        # an integer literal beyond int64
        self.wide = integer and (len(text) > 20 or not _INT64_MIN <= int(text) <= _INT64_MAX)
        info = infer.parse_float(text)
        self.lossy = info is None or not info.lossless  # does not survive float64
        self.no_decimal = exp or neg_zero  # no decimal form
        self.nan = False  # "NaN" / "Infinity" / "-Infinity" strings were seen
        if exp:
            self.int_digits = self.scale = 0
        else:
            whole, _, frac = text.lstrip("-").partition(".")
            self.int_digits = len(whole.lstrip("0"))
            self.scale = len(frac)

    @classmethod
    def of_int(cls, v: int) -> Num:
        n = cls.__new__(cls)
        n.frac = n.no_decimal = n.nan = False
        n.scale = 0
        n.wide = n.lossy = False
        n.int_digits = 0
        n.add_int(v)
        return n

    def add_int(self, v: int) -> None:
        """Fold in an integer value (a literal of at most 40 digits)."""
        a = -v if v < 0 else v
        if not _INT64_MIN <= v <= _INT64_MAX:
            self.wide = True
        if a >= _EXACT and not self.lossy:
            info = infer.parse_float(str(v))
            self.lossy = info is None or not info.lossless
        if a:
            digits = len(str(a))
            if digits > self.int_digits:
                self.int_digits = digits

    def merge(self, o: Num) -> None:
        self.frac |= o.frac
        self.wide |= o.wide
        self.lossy |= o.lossy
        self.no_decimal |= o.no_decimal
        self.int_digits = max(self.int_digits, o.int_digits)
        self.scale = max(self.scale, o.scale)
        self.nan |= o.nan


class Keys:
    """The fields of objects in first-seen order."""

    __slots__ = ("index", "kinds", "names")

    def __init__(self) -> None:
        self.names: List[str] = []
        self.kinds: List[Any] = []
        self.index: Dict[str, int] = {}

    def absorb(self, key: str, v: Any) -> None:
        i = self.index.get(key)
        if i is None:
            self.index[key] = len(self.names)
            self.names.append(key)
            self.kinds.append(observe(v))
        else:
            self.kinds[i] = absorb(self.kinds[i], v)

    def merge(self, other: Keys) -> None:
        for name, kind in zip(other.names, other.kinds):
            i = self.index.get(name)
            if i is None:
                self.index[name] = len(self.names)
                self.names.append(name)
                self.kinds.append(kind)
            else:
                self.kinds[i] = merge(self.kinds[i], kind)


class Arr:
    """What the items of arrays have looked like."""

    __slots__ = ("item",)

    def __init__(self, item: Any) -> None:
        self.item = item


def observe(v: Any) -> Any:
    """The kind of one value."""
    if v is None:
        return NULL
    t = type(v)
    if t is bool:
        return BOOL
    if t is int:
        return Num.of_int(v)
    if t is JsonNumber or t is JsonInt:
        return Num(v)
    if t is str:
        return NANTEXT if v in NAN_TEXT else STR
    if t is dict:
        keys = Keys()
        for k, x in v.items():
            keys.absorb(k, x)
        return keys
    if t is list:
        arr = Arr(NULL)
        for x in v:
            arr.item = absorb(arr.item, x)
        return arr
    raise TypeError(f"not a JSON value: {v!r}")


def absorb(k: Any, v: Any) -> Any:
    """Fold one more value into what a field has looked like (the new kind)."""
    if v is None or k is MIXED:
        return k
    t = type(v)
    if t is int:
        if type(k) is Num:
            k.add_int(v)
            return k
    elif t is JsonNumber or t is JsonInt:
        if type(k) is Num:
            k.merge(Num(v))
            return k
    elif t is str:
        if k is STR:
            return k
    elif t is bool:
        if k is BOOL:
            return k
    elif t is dict:
        if type(k) is Keys:
            for key, x in v.items():
                k.absorb(key, x)
            return k
    elif t is list and type(k) is Arr:
        for x in v:
            k.item = absorb(k.item, x)
        return k
    return merge(k, observe(v))


def merge(a: Any, b: Any) -> Any:
    """Two kinds as one (the new kind)."""
    if b is NULL or a is MIXED:
        return a
    if a is NULL:
        return b
    if a is NANTEXT:
        if type(b) is Num:
            b.nan = True
            return b
        return b if b is NANTEXT or b is STR else MIXED
    if (a is BOOL and b is BOOL) or (a is STR and (b is STR or b is NANTEXT)):
        return a
    ta = type(a)
    if ta is Num:
        if type(b) is Num:
            a.merge(b)
            return a
        if b is NANTEXT:
            a.nan = True
            return a
    elif ta is Keys:
        if type(b) is Keys:
            a.merge(b)
            return a
    elif ta is Arr and type(b) is Arr:
        a.item = merge(a.item, b.item)
        return a
    return MIXED


# ---------------------------------------------------------------------------------
# Plans: how each column, struct field and list item is decoded
# ---------------------------------------------------------------------------------


class Misfit(Exception):
    """A value that does not fit its plan (``shown``: a short text of it)."""

    def __init__(self, value: Any = None, shown: Optional[str] = None) -> None:
        super().__init__()
        self.shown = shown if shown is not None else _shown(value)


def _shown(v: Any) -> str:
    """A short JSON text of a value, cut after 57 bytes as the native engine cuts it."""
    try:
        s = dump(v)
        if not s.isascii() and _SURROGATE.search(s):
            raise ValueError
    except (TypeError, ValueError):
        s = json.dumps(v, default=str)  # escapes a lone surrogate
    b = s.encode("utf-8", "surrogatepass")
    if len(b) > 60:
        cut = 57
        while cut and b[cut] & 0xC0 == 0x80:
            cut -= 1
        s = b[:cut].decode("utf-8", "surrogatepass") + "..."
    return s


def _debug_str(s: str) -> str:
    """A string as Rust's ``{:?}`` writes it (for type names in messages)."""
    out = []
    for c in s:
        if c in ('"', "\\"):
            out.append("\\" + c)
        elif c == "\n":
            out.append("\\n")
        elif c == "\r":
            out.append("\\r")
        elif c == "\t":
            out.append("\\t")
        elif c == "\0":
            out.append("\\0")
        elif ord(c) < 0x20 or ord(c) == 0x7F:
            out.append(f"\\u{{{ord(c):x}}}")
        else:
            out.append(c)
    return '"' + "".join(out) + '"'


class Plan:
    """How one column, struct field or list item is decoded: ``kind`` is int, float,
    decimal, text, number (numbers kept as text), json (any value as its JSON text),
    bool, struct or list."""

    __slots__ = ("index", "item", "kind", "names", "plans", "precision", "scale")

    def __init__(self, kind: str, precision: int = 0, scale: int = 0) -> None:
        self.kind = kind
        self.precision, self.scale = precision, scale
        self.names: List[str] = []
        self.plans: List[Plan] = []
        self.index: Dict[str, int] = {}
        self.item: Optional[Plan] = None

    @property
    def mark(self) -> Optional[str]:
        """The ``vegg.json`` metadata of the field (None for none)."""
        return "number" if self.kind == "number" else "value" if self.kind == "json" else None

    @property
    def label(self) -> str:
        """The type's name in reports, ``head`` and ``schema`` (as every engine names it)."""
        k = self.kind
        if k == "int":
            return "int64"
        if k == "float":
            return "float64"
        if k == "decimal":
            return f"decimal({self.precision},{self.scale})"
        if k == "bool":
            return "bool"
        if k == "struct":
            return "struct<" + ", ".join(f"{n}: {p.label}" for n, p in zip(self.names, self.plans)) + ">"
        if k == "list":
            return f"list<{self.item.label}>"  # type: ignore[union-attr]
        return "string"

    @property
    def rust(self) -> str:
        """The type as the native engine's messages name it (arrow-rs)."""
        k = self.kind
        if k == "int":
            return "Int64"
        if k == "float":
            return "Float64"
        if k == "decimal":
            return f"Decimal128({self.precision}, {self.scale})"
        if k == "bool":
            return "Boolean"
        if k == "struct":
            return (
                "Struct(" + ", ".join(f"{_debug_str(n)}: {p.rust}{p._rust_meta()}" for n, p in zip(self.names, self.plans)) + ")"
            )
        if k == "list":
            item = self.item
            return f"List({item.rust}{item._rust_meta()})"  # type: ignore[union-attr]
        return "Utf8"

    def _rust_meta(self) -> str:
        mark = self.mark
        return "" if mark is None else f', metadata: {{"{JSON_TEXT_KEY}": "{mark}"}}'

    def logical(self) -> infer.LogicalType:
        """The engines' type of a column with this plan."""
        k = self.kind
        if k == "decimal":
            return infer.LogicalType("decimal", self.precision, self.scale)
        if k in ("int", "float", "bool"):
            return infer.LogicalType(self.label)
        if k in ("struct", "list"):
            return infer.LogicalType(self.label)  # named by its label (not a CSV type)
        return infer.STRING

    def arrow(self, pa: Any) -> Any:
        """The Arrow type (struct fields and list items carry their marks)."""
        k = self.kind
        if k == "int":
            return pa.int64()
        if k == "float":
            return pa.float64()
        if k == "decimal":
            return pa.decimal128(self.precision, self.scale)
        if k == "bool":
            return pa.bool_()
        if k == "struct":
            return pa.struct([p.field(pa, n) for n, p in zip(self.names, self.plans)])
        if k == "list":
            return pa.list_(self.item.field(pa, "item"))  # type: ignore[union-attr]
        return pa.string()

    def field(self, pa: Any, name: str) -> Any:
        mark = self.mark
        return pa.field(name, self.arrow(pa), metadata={JSON_TEXT_KEY: mark} if mark else None)

    def converter(self) -> Callable[[Any], Any]:
        """A value read from JSON as this plan holds it (raises :class:`Misfit`): ints,
        floats, Decimals, text, JsonText for numbers or JSON kept as text, bools,
        dicts holding every field of the struct in order, lists."""
        k = self.kind
        if k == "int":

            def conv(v: Any) -> Any:
                if v is None or (type(v) is int and _INT64_MIN <= v <= _INT64_MAX):
                    return v
                raise Misfit(v)

        elif k == "float":

            def conv(v: Any) -> Any:
                if v is None:
                    return None
                t = type(v)
                if t is int:
                    if -_EXACT < v < _EXACT:
                        return float(v)
                    info = infer.parse_float(str(v))
                elif t is JsonNumber or t is JsonInt:
                    info = infer.parse_float(v)
                elif t is str and v in NAN_TEXT:
                    return NAN_TEXT[v]
                else:
                    raise Misfit(v)
                if info is not None and info.lossless:
                    return info.value
                raise Misfit(v)

        elif k == "decimal":
            scale, limit = self.scale, 10**self.precision
            unit = 10**scale

            def conv(v: Any) -> Any:
                if v is None:
                    return None
                t = type(v)
                if t is int:
                    x: Optional[int] = v * unit
                elif t is JsonNumber or t is JsonInt:
                    x = parse_decimal(v, scale)
                else:
                    raise Misfit(v)
                if x is None or not -limit < x < limit:
                    raise Misfit(v)
                return Decimal(x).scaleb(-scale, context=infer.DECIMAL_CONTEXT)

        elif k == "text":

            def conv(v: Any) -> Any:
                if v is None:
                    return None
                if type(v) is str and (v.isascii() or not _SURROGATE.search(v)):
                    return v
                raise Misfit(v)

        elif k == "number":

            def conv(v: Any) -> Any:
                if v is None:
                    return None
                t = type(v)
                if t is int:
                    return JsonText(v)
                if t is JsonNumber or t is JsonInt:
                    return JsonText(number_text(v))
                if t is str and v in NAN_TEXT:
                    return v  # stays a string in JSON output: JSON has no such numbers
                raise Misfit(v)

        elif k == "json":

            def conv(v: Any) -> Any:
                if v is None:
                    return None
                text = dump(v)
                if not text.isascii() and _SURROGATE.search(text):
                    raise Misfit(v)
                return JsonText(text)

        elif k == "bool":

            def conv(v: Any) -> Any:
                if v is None or v is True or v is False:
                    return v
                raise Misfit(v)

        elif k == "struct":
            index = self.index
            fields = [(n, p.converter()) for n, p in zip(self.names, self.plans)]

            def conv(v: Any) -> Any:
                if v is None:
                    return None
                if type(v) is not dict:
                    raise Misfit(v)
                for key in v:
                    if key not in index:
                        raise Misfit(shown=f'with the new key "{key}"')
                return {n: c(v.get(n)) for n, c in fields}

        else:  # list
            item = self.item.converter()  # type: ignore[union-attr]

            def conv(v: Any) -> Any:
                if v is None:
                    return None
                if type(v) is not list:
                    raise Misfit(v)
                return [item(x) for x in v]

        return conv


def _num_plan(n: Num) -> Plan:
    if not n.frac and not n.nan:
        if not n.wide:
            return Plan("int")
        if n.int_digits <= 38:
            return Plan("decimal", max(1, n.int_digits), 0)
        return Plan("number")
    if not n.lossy:
        return Plan("float")
    digits = n.int_digits + n.scale
    if not n.nan and not n.no_decimal and 1 <= digits <= 38:
        return Plan("decimal", digits, n.scale)
    return Plan("number")


def plan_of(kind: Any, path: str, warnings: List[str]) -> Plan:
    """The plan of a field that has looked like ``kind`` (``path`` names it in the
    warnings about fields that mix JSON types)."""
    if kind is BOOL:
        return Plan("bool")
    t = type(kind)
    if t is Num:
        return _num_plan(kind)
    if kind is MIXED:
        warnings.append(f"column '{path}' mixes JSON types; values are stored as JSON text")
        return Plan("json")
    if t is Keys:
        if not kind.names:  # an object that never has a field cannot be a column of its own
            return Plan("json")
        return layout(kind, path, warnings)
    if t is Arr:
        p = Plan("list")
        p.item = plan_of(kind.item, f"{path}[]", warnings)
        return p
    return Plan("text")  # null, strings, and only NaN strings


def layout(keys: Keys, path: str, warnings: List[str]) -> Plan:
    """The struct plan of objects whose fields have looked like ``keys`` (the top
    level when ``path`` is empty)."""
    p = Plan("struct")
    p.names = list(keys.names)
    p.index = dict(keys.index)
    p.plans = [plan_of(kind, name if not path else f"{path}.{name}", warnings) for name, kind in zip(keys.names, keys.kinds)]
    return p
