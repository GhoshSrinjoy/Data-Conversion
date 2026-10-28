"""Standard-library engine: CSV/TSV/PSV, JSON Lines, JSON arrays and DCZ.

Slower than the native and arrow engines, but it runs on any Python 3.8+ and
applies exactly the same lossless type rules and output formatting.
"""

from __future__ import annotations

import csv
import io
import json
import math
import os
import time
from datetime import date, datetime, timedelta
from datetime import time as dtime
from decimal import Decimal
from itertools import islice
from pathlib import Path
from typing import IO, Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple

from .. import _csvtok, _io, dcz, formats, infer
from .._sniff import Dialect, sniff, sniff_bytes
from .._util import atomic_output
from ..errors import DataError, FormatError, OptionsError, OutputExistsError
from ..options import ConvertOptions
from ..report import Report

Progress = Optional[Callable[[int, int], Any]]
SAMPLE_ROWS = 100_000
DELIMITED = ("csv", "tsv", "psv", "dcz")
#: JSON values nested deeper than this are refused (the native reader's limit).
MAX_JSON_DEPTH = 128


class _Restart(Exception):
    def __init__(self, reason: str, column: Optional[int] = None, value: str = "") -> None:
        super().__init__(reason)
        self.reason = reason
        self.column = column
        self.value = value


# ---------------------------------------------------------------------------------
# Value formatting (identical to the native writers)
# ---------------------------------------------------------------------------------

_SPECIAL_FLOATS = {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}


class JsonNumber(str):
    """A JSON number literal kept exactly as written: every non-integer and ``-0``."""

    __slots__ = ()


class JsonInt(JsonNumber):
    """An integer literal too long for any numeric type (kept as its text)."""

    __slots__ = ()


class JsonText(str):
    """The JSON text of a value held in a text column (a field mixing JSON types, or
    an Arrow column marked ``dataconv.json = value``): JSON output writes it as the
    JSON it holds, CSV output as its text."""

    __slots__ = ()


#: Arrow field metadata of text columns that hold JSON: "number" (numbers kept as
#: their text) or "value" (the JSON text of any value), as the native engine sets it
JSON_TEXT_KEY = "dataconv.json"

_encode_str = json.encoder.encode_basestring  # json.dumps(s, ensure_ascii=False) of a str, without its set-up


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


def _ryu_notation(shortest: str, top: int, bottom: int) -> str:
    """Shortest digits, given in any notation (``1.5e-05``, ``3.4028235e+38``,
    ``16777216``), in the notation of the native writers (Rust's ``ryu``): plain from
    ``10**bottom`` up to ``10**top`` with at least one decimal (``100.0``), otherwise
    ``1e-7`` or ``1.5e22``, without a ``+`` sign or leading zeros in the exponent."""
    sign = "-" if shortest[:1] == "-" else ""
    mant, _, exp = shortest.lstrip("-").partition("e")
    whole, _, frac = mant.partition(".")
    digits = (whole + frac).lstrip("0")
    # the value is 0.<digits> * 10**point
    point = len(whole) + int(exp or 0) - (len(whole) + len(frac) - len(digits))
    digits = digits.rstrip("0")
    n = len(digits)
    if not n:
        return sign + "0.0"
    if n <= point <= top:
        return sign + digits + "0" * (point - n) + ".0"
    if 0 < point <= top:
        return sign + digits[:point] + "." + digits[point:]
    if bottom < point <= 0:
        return sign + "0." + "0" * -point + digits
    return sign + digits[0] + ("." + digits[1:] if n > 1 else "") + "e" + str(point - 1)


def float_repr(v: float) -> str:
    """A finite float as the native writers print it: the shortest digits that read
    back to the same double, plain from 1e-5 up to 1e16 (``repr`` switches to an
    exponent below 1e-4 and writes it as ``1e-07``, ``1e+22``)."""
    r = repr(v)
    return r if "e" not in r else _ryu_notation(r, 16, -5)


def single_repr(shortest: str) -> str:
    """A finite float32 as the native writers print it, from its own shortest digits
    (pyarrow's text of the value): plain from 1e-6 up to 1e13."""
    return _ryu_notation(shortest, 13, -6)


def _float_text(v: float) -> str:
    if v != v:
        return "NaN"
    if v in (math.inf, -math.inf):
        return "inf" if v > 0 else "-inf"
    return float_repr(v)


def _micros(d: timedelta) -> int:
    return (d.days * 86400 + d.seconds) * 1_000_000 + d.microseconds


def _temporal_text(v: Any) -> str:
    """Python date/time values in the native writers' format."""
    if isinstance(v, datetime):
        offset = v.utcoffset()
        local = _micros(v.replace(tzinfo=None) - datetime(1970, 1, 1))
        if offset is None:
            return infer.format_timestamp(local, "us", None)
        return infer.format_timestamp(local - _micros(offset), "us", _micros(offset) // 60_000_000)
    if isinstance(v, date):
        return infer.format_date(v.toordinal() - 719163)
    if isinstance(v, dtime):
        return infer.format_time(_micros(timedelta(hours=v.hour, minutes=v.minute, seconds=v.second, microseconds=v.microsecond)), "us")
    return infer.format_duration(_micros(v), "us")


def json_text(v: Any) -> str:
    """Compact JSON text of a value (number literals read from JSON keep their text;
    NaN and infinities are the strings "NaN", "Infinity" and "-Infinity")."""
    if v is None:
        return "null"
    if v is True:
        return "true"
    if v is False:
        return "false"
    if isinstance(v, JsonNumber):
        return number_text(v)
    if isinstance(v, JsonText):
        return str(v)
    if isinstance(v, str):
        return _encode_str(v)  # type: ignore[no-any-return]
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if v != v:
            return '"NaN"'
        if v in (math.inf, -math.inf):
            return '"Infinity"' if v > 0 else '"-Infinity"'
        return float_repr(v)
    if isinstance(v, dict):
        return "{" + ",".join(_encode_str(str(k)) + ":" + json_text(x) for k, x in v.items()) + "}"
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(json_text(x) for x in v) + "]"
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, (datetime, date, dtime, timedelta)):
        return json.dumps(_temporal_text(v))
    if isinstance(v, (bytes, bytearray)):
        return json.dumps(bytes(v).hex())
    return _encode_str(str(v))  # type: ignore[no-any-return]


def csv_text(v: Any) -> Optional[str]:
    """Text of a value in CSV; None means "write an empty unquoted field"."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, JsonNumber):
        return number_text(v)
    if isinstance(v, str):
        return v
    if isinstance(v, float):
        return _float_text(v)
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, (datetime, date, dtime, timedelta)):
        return _temporal_text(v)
    if isinstance(v, (dict, list)):
        return json_text(v)
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).hex()
    return str(v)


def csv_line(values: Sequence[Any], delimiter: str, terminator: str) -> str:
    out = []
    specials = (delimiter, '"', "\n", "\r")
    for v in values:
        t = csv_text(v)
        if t is None:
            out.append("")
        elif t == "" or any(s in t for s in specials) or t[0] == " " or t[-1] == " ":
            out.append('"' + t.replace('"', '""') + '"')
        else:
            out.append(t)
    return delimiter.join(out) + terminator


def json_value(v: Any) -> str:
    if v is None:
        return "null"
    if v.__class__ is str:
        return _encode_str(v)  # type: ignore[no-any-return]
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, JsonNumber):
        return number_text(v)
    if isinstance(v, JsonText):
        return str(v)
    if isinstance(v, str):
        return _encode_str(v)  # type: ignore[no-any-return]
    if isinstance(v, int):
        return str(v)
    return json_text(v)


def json_keys(names: Sequence[str]) -> List[str]:
    """The ``"name":`` prefix of each field of a JSON object."""
    return [_encode_str(n) + ":" for n in names]


def json_object(names: Sequence[str], values: Sequence[Any], keys: Optional[Sequence[str]] = None) -> str:
    keys = json_keys(names) if keys is None else keys
    return "{" + ",".join([k + json_value(v) for k, v in zip(keys, values)]) + "}"


# ---------------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------------


class Source:
    names: List[str]
    types: List[infer.LogicalType]
    bad: List[str]
    blank: int = 0
    irregular: int = 0
    warnings: List[str]

    def rows(self) -> Iterator[List[Any]]:
        raise NotImplementedError

    def consumed(self) -> int:
        return 0


#: Python 3.12+: the csv module reads an empty unquoted field as None, as the tokenizer does
_QUOTE_NOTNULL = getattr(csv, "QUOTE_NOTNULL", None)
_RUN = 1 << 15  # characters of regular records read by the csv module at a time
_AHEAD = 1 << 16  # at most this many characters read by the tokenizer before trying a fast run again


def _regular_reader(d: _csvtok.Dialect, ncols: int) -> Optional[Callable[[str], Optional[List[List[Optional[str]]]]]]:
    """A reader of runs of regular records (see :func:`_csvtok.regular_run`) through the
    csv module, which splits them exactly like the tokenizer: their rows, or None where
    its reading could differ (a quoted empty field before Python 3.12, a NUL before 3.11,
    a field over the csv module's size limit) or a row is malformed (its text is kept)."""
    if not d.fast or not d.text:
        return None
    kw: Dict[str, Any] = {"delimiter": d.delimiter, "quotechar": d.quote, "doublequote": True, "strict": False}
    if _QUOTE_NOTNULL is not None:
        kw["quoting"] = _QUOTE_NOTNULL

    def read(text: str) -> Optional[List[List[Optional[str]]]]:
        if _QUOTE_NOTNULL is None and '""' in text:
            return None
        try:
            rows: List[Any] = list(csv.reader(io.StringIO(text, newline=""), **kw))
        except csv.Error:
            return None
        for k, row in enumerate(rows):
            if len(row) != ncols:
                if row:
                    return None
                rows[k] = [None]  # a blank line
            elif _QUOTE_NOTNULL is None:
                rows[k] = [v if v else None for v in row]
        return rows

    return read


def _chunk_records(chunk: str, d: _csvtok.Dialect, pos: int, read: Any) -> Iterator[Tuple[List[Optional[str]], int, str, int, int]]:
    """Records of ``chunk`` from ``pos`` as :meth:`CsvSource.records` yields them: runs
    of regular records through ``read`` (a run at most about ``_RUN`` long, so that a
    malformed row sends little to the tokenizer), every other record through the
    tokenizer, which reads on further each time so that dense irregular records do not
    each cost a fast-path check."""
    n = len(chunk)
    ahead = 0
    while pos < n:
        stop = n
        if read is not None:
            r = _csvtok.regular_run(chunk, d, pos, 1 << 10 if ahead else _RUN, most=_RUN)[0]
            if r > pos:
                rows = read(chunk[pos:r])
                if rows is not None:
                    for values in rows:
                        yield values, 0, chunk, 0, 0
                    if r - pos >= 1 << 12:
                        ahead = 0
                    pos = r
                    continue
                stop = r
            else:
                stop = r + 1 + ahead
                ahead = min(max(ahead * 2, 1 << 10), _AHEAD)
        for values, start, end, _, flags in _csvtok.tokenize(chunk, d, pos):
            yield values, flags, chunk, start, end
            pos = end
            if end >= stop:
                break


def _open_csv_text(path: Path, fmt: str, encoding: str) -> IO[str]:
    if fmt == "dcz":
        enc = "utf-8-sig" if encoding.lower() in ("utf-8", "utf8") else encoding
        raw = io.BufferedReader(dcz.DczReader(path), 1 << 20)
        return io.TextIOWrapper(raw, encoding=enc, errors=_io.text_errors(enc), newline="")
    return _io.open_text(path, encoding)


class CsvSource(Source):
    """Records split by the quote rules every engine shares (see :mod:`dataconv._csvtok`)."""

    def __init__(self, path: Path, fmt: str, dialect: Dialect, o: ConvertOptions, text_output: bool) -> None:
        self.path, self.fmt, self.dialect, self.o = path, fmt, dialect, o
        cols = dialect.columns
        if o.columns:
            missing = [c for c in o.columns if c not in cols]
            if missing:
                raise FormatError(f"no column named {missing[0]!r}")
            self.projection = [cols.index(c) for c in o.columns]
        else:
            self.projection = list(range(len(cols)))
        self.names = [cols[i] for i in self.projection]
        self.ncols = len(cols)
        self._all_columns = self.projection == list(range(self.ncols))
        self.tok = _csvtok.Dialect.of(dialect)
        self.mode = "string" if text_output and not o.schema else o.types
        self.types = [infer.STRING] * len(self.names)
        self.bad = []
        self.blank = 0
        self.irregular = 0
        self.warnings = []
        self.forced_at: Optional[int] = None  # a quote read as a stray only by its size
        self._cuts: Dict[int, Any] = {}  # chunks already cut, reused by the next pass (see Chunker)
        self._file: Optional[IO[str]] = None

    def _chunks(self) -> Iterator[Tuple[str, int]]:
        """(chunk, offset of its first record after the leading ones) of the whole file."""
        f = _open_csv_text(self.path, self.fmt, self.dialect.encoding)
        self._file = f
        d = self.tok
        skip = self.dialect.leading_records
        done = 0  # UTF-8 bytes of the chunks so far (where a forced stray quote is reported)
        try:
            chunker = _csvtok.Chunker(f, 1 << 20, d, known=self._cuts)
            for chunk in chunker:
                done += len(chunk) if chunk.isascii() else len(chunk.encode("utf-8", "surrogatepass"))
                if chunker.forced and self.forced_at is None:
                    self.forced_at = done
                pos = 0
                if skip:
                    pos, skipped = _csvtok.skip_records(chunk, d, skip)
                    skip -= skipped
                yield chunk, pos
        except UnicodeDecodeError as exc:
            raise _Restart("encoding") from exc
        finally:
            f.close()

    def records(self) -> Iterator[Tuple[List[Optional[str]], int, str, int, int]]:
        """(values, flags, chunk, start, end) of every record after the leading ones;
        ``chunk[start:end]`` is the original text of a record the tokenizer read (runs of
        regular records go through the csv module at C speed, and are all well-formed)."""
        read = _regular_reader(self.tok, self.ncols)
        for chunk, pos in self._chunks():
            yield from _chunk_records(chunk, self.tok, pos, read)

    def consumed(self) -> int:
        f = self._file
        try:
            raw = getattr(f, "buffer", None)
            return int(raw.tell()) if raw is not None and not raw.closed else 0
        except (OSError, ValueError, AttributeError):
            return 0

    def _fields(self, values: List[Optional[str]], flags: int, chunk: str, start: int, end: int) -> Optional[List[Optional[str]]]:
        # in a single-column file a blank line is a row whose value is missing
        if self.ncols != 1 and len(values) == 1 and values[0] is None:
            self.blank += 1
            return None
        if flags:
            self.irregular += 1
        n = len(values)
        malformed = flags & _csvtok.MALFORMED  # a stray quote, whatever the width
        if n != self.ncols or malformed:
            if self.o.bad_lines == "pad" and n < self.ncols and not malformed:
                values = values + [None] * (self.ncols - n)
            elif self.o.bad_lines == "error":
                shown = chunk[start:end][:200]
                if flags & _csvtok.UNTERMINATED:
                    raise DataError(f"a quote is never closed: {shown!r}")
                if malformed:
                    raise DataError(f"a quoted field spanning lines has text after its closing quote: {shown!r}")
                raise DataError(f"record with {n} fields where {self.ncols} were expected: {shown!r}")
            else:
                self.bad.append(chunk[start:end])
                return None
        if self._all_columns:
            return values
        return [values[i] for i in self.projection]

    def raw_rows(self) -> Iterator[List[Optional[str]]]:
        """Values of every well-formed (or padded) row, after the bad-lines policy."""
        for values, flags, chunk, start, end in self.records():
            fields = self._fields(values, flags, chunk, start, end)
            if fields is not None:
                yield fields

    def count(self) -> int:
        """Records after the leading ones, as the converters split them (blank lines
        count only in a single-column file).  A run of regular records without blank
        lines is counted from its line breaks, at C speed."""
        single = self.ncols == 1
        d = self.tok
        total = 0
        for chunk, pos in self._chunks():
            n = len(chunk)
            ahead = 0
            while pos < n:
                r, breaks = _csvtok.regular_run(chunk, d, pos, 1 << 10 if ahead else _csvtok.CUT_WINDOW, count=True)
                if r > pos and (single or not _csvtok.may_hold_blank(chunk, d, pos, r)):
                    total += breaks + (1 if r == n and chunk[-1:] not in (d.nl, d.cr) else 0)
                    if r - pos >= 1 << 12:
                        ahead = 0
                    pos = r
                    continue
                stop = r if r > pos else r + 1 + ahead
                if r == pos:
                    ahead = min(max(ahead * 2, 1 << 10), _AHEAD)
                for values, _, end, _, _ in _csvtok.tokenize(chunk, d, pos):
                    if single or not _csvtok.is_blank(values):
                        total += 1
                    pos = end
                    if end >= stop:
                        break
        return total

    def infer(self, full: bool) -> None:
        if self.mode == "string" and not self.o.schema:
            return
        stats = [infer.ColumnStats() for _ in self.names]
        nulls = self.o.null_values
        for n, row in enumerate(self.raw_rows()):
            if not full and n >= SAMPLE_ROWS:
                break
            for s, v in zip(stats, row):
                s.observe("" if v is None else v, self.mode, nulls)
        self.bad.clear()
        self.blank = self.irregular = 0
        types = []
        for name, s in zip(self.names, stats):
            if name in self.o.schema:
                types.append(infer.parse_type(self.o.schema[name]))
                continue
            ty = s.decide() if self.mode != "string" else infer.STRING
            note = s.explain(name)
            if note and note not in self.warnings:
                self.warnings.append(note)
            types.append(ty)
        self.types = types

    def rows(self) -> Iterator[List[Any]]:
        convs = [infer.converter(t, self.mode, self.o.null_values) for t in self.types]
        for row in self.raw_rows():
            out = []
            for j, (conv, v) in enumerate(zip(convs, row)):
                try:
                    out.append(conv("" if v is None else v, v is None))
                except (ValueError, KeyError):
                    raise _Restart("violation", j, v or "") from None
            yield out


# --- JSON ------------------------------------------------------------------------


def _int_literal(text: str) -> Any:
    # -0 is never an integer (only a float keeps its sign); integers longer than any
    # numeric type are only ever kept as text (and int() refuses very long ones)
    if text == "-0":
        return JsonNumber(text)
    return JsonInt(text) if len(text) > 40 else int(text)


_DECODER = json.JSONDecoder(parse_float=JsonNumber, parse_int=_int_literal, parse_constant=_SPECIAL_FLOATS.__getitem__)


def _too_deep(v: Any) -> bool:
    stack = [(v, 1)]
    while stack:
        x, depth = stack.pop()
        if depth > MAX_JSON_DEPTH:
            return True
        children = x.values() if isinstance(x, dict) else x
        for c in children:
            if isinstance(c, (dict, list)):
                stack.append((c, depth + 1))
    return False


class _Misfit(Exception):
    """A JSON value does not fit the column's planned type."""


class _JsonStats:
    """What a JSON field holds, deciding its column type by the lossless rules shared
    with the native engine."""

    __slots__ = ("big", "fields", "float_ok", "frac_digits", "int_digits", "kinds", "lists", "negzero", "nonint",
                 "plain", "specials")

    def __init__(self) -> None:
        self.kinds: set = set()  # str, num, bool, nested
        self.specials = 0  # the strings "NaN", "Infinity" and "-Infinity"
        self.big = False  # an integer beyond int64
        self.nonint = False  # a non-integer literal, -0, or a NaN/Infinity constant
        self.float_ok = True  # every number survives float64
        self.plain = True  # every number is in plain notation (decimal is possible)
        self.negzero = False  # -0 (decimal cannot keep the sign)
        self.int_digits = 0
        self.frac_digits = 0
        self.fields = False  # an object with at least one field
        self.lists = False

    def observe(self, v: Any) -> None:
        if v is None:
            return
        if v is True or v is False:
            self.kinds.add("bool")
        elif isinstance(v, JsonInt):
            self.kinds.add("num")
            self.big = True
            self.float_ok = False
            self.int_digits = max(self.int_digits, len(v.lstrip("-")))
        elif isinstance(v, JsonNumber):
            self.kinds.add("num")
            self.nonint = True
            info = infer.parse_float(v)
            if info is None:
                self.float_ok = self.plain = False
                return
            if not info.lossless:
                self.float_ok = False
            if not info.plain:
                self.plain = False
            if v.startswith("-") and info.value == 0:
                self.negzero = True
            self.int_digits = max(self.int_digits, info.int_digits)
            self.frac_digits = max(self.frac_digits, info.frac_digits)
        elif isinstance(v, int):
            self.kinds.add("num")
            if not infer.INT64_MIN <= v <= infer.INT64_MAX:
                self.big = True
            if abs(v) > (1 << 53):
                self.float_ok = False
            self.int_digits = max(self.int_digits, len(str(abs(v))))
        elif isinstance(v, float):  # a bare NaN / Infinity constant
            self.kinds.add("num")
            self.nonint = True
            self.plain = False
        elif isinstance(v, str):
            if v in _SPECIAL_FLOATS:
                self.specials += 1
            else:
                self.kinds.add("str")
        else:
            self.kinds.add("nested")
            if isinstance(v, dict):
                self.fields = self.fields or bool(v)
            else:
                self.lists = True

    def decide(self) -> Tuple[infer.LogicalType, str]:
        """(type, how values are produced)."""
        kinds = set(self.kinds)
        if self.specials:
            if kinds == {"num"}:
                # numbers plus NaN / Infinity strings: floats when every number survives
                # float64, else the numbers' text with the strings as written
                if self.float_ok:
                    return infer.LogicalType("float64"), "float64"
                return infer.STRING, "number_text"
            kinds.add("str")
        if not kinds:
            return infer.STRING, "string"
        if len(kinds) > 1:
            return infer.STRING, "json_text"
        kind = kinds.pop()
        if kind == "str":
            return infer.STRING, "string"
        if kind == "bool":
            return infer.LogicalType("bool"), "bool"
        if kind == "nested":
            if not self.fields and not self.lists:
                return infer.STRING, "json_text"  # objects that never have a field
            return infer.LogicalType("json"), "json"
        if not self.nonint:
            if not self.big:
                return infer.LogicalType("int64"), "int64"
            if self.int_digits <= 38:
                return infer.LogicalType("decimal", max(1, self.int_digits), 0), "decimal"
            return infer.STRING, "number_text"
        if self.float_ok:
            return infer.LogicalType("float64"), "float64"
        precision = self.int_digits + self.frac_digits
        if self.plain and not self.negzero and precision <= 38:
            return infer.LogicalType("decimal", max(1, precision), self.frac_digits), "decimal"
        return infer.STRING, "number_text"

    def mixed(self) -> bool:
        return self.decide()[1] == "json_text" and (len(self.kinds | ({"str"} if self.specials else set())) > 1)


def _json_converter(how: str, ty: infer.LogicalType) -> Callable[[Any], Any]:
    """Value of a JSON field for a column produced ``how``; raises _Misfit for a value
    the planned type cannot hold."""
    if how == "json_text":
        return lambda v: None if v is None else JsonText(json_text(v))
    if how == "string":
        def conv(v: Any) -> Any:
            if v is None or (isinstance(v, str) and not isinstance(v, JsonNumber)):
                return v
            raise _Misfit
    elif how == "bool":
        def conv(v: Any) -> Any:
            if v is None or v is True or v is False:
                return v
            raise _Misfit
    elif how == "int64":
        def conv(v: Any) -> Any:
            if v is None or (type(v) is int and infer.INT64_MIN <= v <= infer.INT64_MAX):
                return v
            raise _Misfit
    elif how == "float64":
        def conv(v: Any) -> Any:
            if v is None or type(v) is float:
                return v
            if isinstance(v, JsonNumber) and not isinstance(v, JsonInt):
                info = infer.parse_float(v)
                if info is not None and info.lossless:
                    return info.value
            elif type(v) is int and abs(v) <= (1 << 53):
                return float(v)
            elif isinstance(v, str) and v in _SPECIAL_FLOATS:
                return _SPECIAL_FLOATS[v]
            raise _Misfit
    elif how == "decimal":
        limit = 10**ty.precision
        quant = Decimal(1).scaleb(-ty.scale)

        def conv(v: Any) -> Any:
            if v is None:
                return None
            if isinstance(v, JsonInt):
                raise _Misfit
            if isinstance(v, JsonNumber):
                info = infer.parse_float(v)
                if info is None or not info.plain or info.frac_digits > ty.scale or (v.startswith("-") and info.value == 0):
                    raise _Misfit
            elif type(v) is not int:
                raise _Misfit
            d = Decimal(v).quantize(quant, context=infer.DECIMAL_CONTEXT)
            if d.scaleb(ty.scale, context=infer.DECIMAL_CONTEXT).copy_abs() >= limit:
                raise _Misfit
            return d
    elif how == "number_text":
        # numbers as their text (JSON output writes them as numbers again); the strings
        # NaN / Infinity / -Infinity as written (they stay strings in JSON output)
        def conv(v: Any) -> Any:
            if v is None:
                return None
            if isinstance(v, JsonNumber):
                return JsonNumber(number_text(v))
            if type(v) is int:
                return JsonNumber(v)
            if type(v) is float:
                return json_text(v).strip('"')
            if type(v) is str and v in _SPECIAL_FLOATS:
                return v
            raise _Misfit
    else:  # nested values, written as JSON
        def conv(v: Any) -> Any:
            if v is None or isinstance(v, (dict, list)):
                return v
            raise _Misfit
    return conv


class JsonSource(Source):
    """JSON Lines or a JSON array of objects (streamed element by element).

    Column types follow the lossless JSON rules shared with the native engine:
    integers within int64 are int64, longer ones exact decimals (or text); other
    numbers are float64 only when every value survives it, else exact decimals or
    their text; a field mixing kinds (string, number, bool, nested) holds each
    value's JSON text.  Keys or values the sample did not show restart the plan
    from a full scan, so nothing is ever dropped."""

    def __init__(self, path: Path, fmt: str, o: ConvertOptions, full: bool = False,
                 nested: Optional[Callable[[str, Any], None]] = None) -> None:
        self.path, self.fmt, self.o = path, fmt, o
        self.bad = []
        self.blank = 0
        self.irregular = 0
        self.warnings = []
        full = full or o.infer == "full"
        stats: Dict[str, _JsonStats] = {}
        self.complete = True
        for n, obj in enumerate(self._objects()):
            if not full and n >= SAMPLE_ROWS:
                self.complete = False
                break
            for k, v in obj.items():
                s = stats.get(k)
                if s is None:
                    s = stats[k] = _JsonStats()
                s.observe(v)
                if nested is not None and isinstance(v, (dict, list)):
                    nested(k, v)
        self.names = list(o.columns) if o.columns else list(stats)
        self.stats = [stats.get(k) or _JsonStats() for k in self.names]
        self.types, self.hows = [], []
        for name, s in zip(self.names, self.stats):
            ty, how = s.decide()
            if how == "json_text" and s.mixed():
                self.warnings.append(f"column '{name}' mixes JSON types; values are stored as JSON text")
            self.types.append(ty)
            self.hows.append(how)

    def _objects(self) -> Iterator[Dict[str, Any]]:
        try:
            for obj in self._parse():
                if not isinstance(obj, dict):
                    obj = {"value": obj}
                for v in obj.values():
                    if isinstance(v, (dict, list)) and _too_deep(v):
                        raise DataError(f"{self.path}: JSON nested more than {MAX_JSON_DEPTH} levels deep")
                yield obj
        except RecursionError:
            raise DataError(f"{self.path}: JSON nested too deeply to read") from None
        except UnicodeDecodeError as exc:
            raise DataError(f"{self.path}: JSON input is not valid UTF-8 ({exc.reason} at byte {exc.start})") from None

    def _parse(self) -> Iterator[Any]:
        raw_decode = _DECODER.raw_decode
        with _io.open_text(self.path, "utf-8") as f:
            if self.fmt == "jsonl":
                for n, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj, end = raw_decode(line)
                    except json.JSONDecodeError as exc:
                        raise DataError(f"{self.path}: invalid JSON on line {n}: {exc.msg}") from None
                    if end != len(line):
                        raise DataError(f"{self.path}: invalid JSON on line {n}: extra data after the value")
                    yield obj
                return
            buf = f.read(1 << 20).lstrip()
            eof = False
            if not buf.startswith("["):
                raise FormatError(f"{self.path}: expected a JSON array of records")
            pos = 1

            def more() -> bool:
                nonlocal buf, pos, eof
                data = f.read(1 << 20)
                if not data:
                    eof = True
                    return False
                buf, pos = buf[pos:] + data, 0
                return True

            while True:
                while True:
                    while pos < len(buf) and buf[pos] in " \t\r\n,":
                        pos += 1
                    if pos < len(buf) or not more():
                        break
                if pos >= len(buf):
                    raise DataError(f"{self.path}: the JSON array is not closed")
                if buf[pos] == "]":
                    return
                while True:
                    try:
                        obj, end = raw_decode(buf, pos)
                    except json.JSONDecodeError as exc:
                        if more():
                            continue
                        raise DataError(f"{self.path}: invalid JSON: {exc.msg}") from None
                    # a value that runs to the end of what was read (a number cut at the
                    # read boundary) may continue: read more before taking it
                    if end >= len(buf) and not eof and more():
                        continue
                    break
                yield obj
                pos = end
                if pos > (1 << 20):
                    buf, pos = buf[pos:], 0

    def rows(self) -> Iterator[List[Any]]:
        convs = [_json_converter(how, ty) for how, ty in zip(self.hows, self.types)]
        names = self.names
        known = None if (self.complete or self.o.columns) else set(names)
        for obj in self._objects():
            if known is not None:
                for k in obj:
                    if k not in known:
                        raise _Restart("columns", None, k)
            out = []
            for j, conv in enumerate(convs):
                v = obj.get(names[j])
                try:
                    out.append(conv(v))
                except _Misfit:
                    raise _Restart("violation", j, json_text(v)[:80]) from None
            yield out


# ---------------------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------------------


class Sink:
    def __init__(self, out: IO[bytes], fmt: str, names: List[str], o: ConvertOptions, dcz_csv: Tuple[str, str] = (",", '"')) -> None:
        self.fmt = fmt
        self.names = names
        self.o = o
        self.first = True
        self.delimiter = o.out_delimiter or formats.DELIMITERS.get(fmt, ",")
        self.terminator = "\r\n" if o.out_crlf else "\n"
        self.dcz: Optional[dcz.DczWriter] = None
        self.raw = out
        if fmt == "dcz":
            self.dcz = dcz.DczWriter(out, (self.delimiter, '"'), o.profile)
        self.buf: List[str] = []
        self.size = 0
        self.keys = json_keys(names)
        if fmt in ("csv", "tsv", "psv", "dcz") and o.out_header is not False:
            self._emit(csv_line(names, self.delimiter, self.terminator))
        elif fmt == "json":
            self._emit("[")

    def _emit(self, text: str) -> None:
        self.buf.append(text)
        self.size += len(text)
        if self.size >= 1 << 20:
            self.flush()

    def flush(self) -> None:
        data = "".join(self.buf).encode("utf-8")
        self.buf, self.size = [], 0
        if self.dcz is not None:
            self.dcz.write(data)
        else:
            self.raw.write(data)

    def write(self, row: Sequence[Any]) -> None:
        if self.fmt in ("csv", "tsv", "psv", "dcz"):
            self._emit(csv_line(row, self.delimiter, self.terminator))
        elif self.fmt == "jsonl":
            self._emit(json_object(self.names, row, self.keys) + "\n")
        elif self.fmt == "json":
            self._emit(("\n" if self.first else ",\n") + json_object(self.names, row, self.keys))
            self.first = False
        else:
            raise FormatError(f"the pure engine cannot write {self.fmt}")

    def close(self) -> None:
        if self.fmt == "json":
            self._emit("\n]\n" if not self.first else "]\n")
        self.flush()
        if self.dcz is not None:
            self.dcz.close()


# ---------------------------------------------------------------------------------
# Rejected rows
# ---------------------------------------------------------------------------------


def _same_file(a: Path, b: Path) -> bool:
    try:
        return a.exists() and b.exists() and os.path.samefile(a, b)
    except OSError:
        return False


class Quarantine:
    """Where malformed rows go: ``<output>.rejected.csv`` unless named.  Checked before
    the conversion starts, written only when it succeeds, and a stale default file
    from an earlier run is removed when nothing was rejected."""

    def __init__(self, src: Path, dst: Path, o: ConvertOptions, delimited: bool, default: Optional[Path] = None) -> None:
        self.explicit = bool(o.quarantine)
        self.path = Path(o.quarantine) if o.quarantine else (default or dst.with_name(dst.name + ".rejected.csv"))
        self.overwrite = o.overwrite
        self.delimited = delimited
        self.active = delimited and o.bad_lines in ("quarantine", "pad")
        if self.active:
            if _same_file(self.path, src):
                raise OptionsError(f"the file for rejected rows ({self.path}) is the input file")
            if self.path.resolve() == dst.resolve():
                raise OptionsError(f"the file for rejected rows ({self.path}) is the output file")
            if self.path.exists() and not o.overwrite:
                raise OutputExistsError(f"{self.path} already exists (pass overwrite=True / --overwrite to replace it)")

    def finish(self, names: Sequence[str], delimiter: str, bad: Sequence[str], report: Report, warnings: List[str]) -> None:
        what = ""
        if bad:
            lines = sum(_csvtok.count_lines(raw) for raw in bad)
            what = f"{len(bad)} malformed rows ({lines} lines)" if lines > len(bad) else f"{len(bad)} malformed rows"
        if bad and self.active:
            with atomic_output(self.path, overwrite=True) as tmp, open(tmp, "w", encoding="utf-8", newline="") as qf:
                qf.write(csv_line(names, delimiter, "\n"))
                for raw in bad:
                    qf.write(raw if raw.endswith(("\n", "\r")) else raw + "\n")
            report.quarantine = str(self.path)
            warnings.append(f"{what} moved to {self.path}")
        elif bad:
            warnings.append(f"{what} skipped")
        elif self.delimited and self.overwrite and not self.explicit and self.path.is_file():
            # a stale file from an earlier run would pass for this run's rejects
            try:
                self.path.unlink()
            except OSError:
                pass


# ---------------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------------


def _dialect(path: Path, fmt: str, o: ConvertOptions) -> Dialect:
    if fmt == "dcz":
        with dcz.DczReader(path) as r:
            raw = r.read(2 << 20)
        return sniff_bytes(raw, len(raw) < (2 << 20), delimiter=o.delimiter, quote=o.quote, escape=o.escape,
                           encoding=o.encoding, header=o.header, skip_rows=o.skip_rows)
    delimiter = o.delimiter or (formats.DELIMITERS[fmt] if fmt in ("tsv", "psv") else None)
    return sniff(path, delimiter=delimiter, quote=o.quote, escape=o.escape, encoding=o.encoding, header=o.header, skip_rows=o.skip_rows)


def plan(src: Path, src_fmt: str, o: ConvertOptions, dialect: Optional[Dialect], text_out: bool,
         consume: Callable[[Source], Any], warnings: List[str]) -> Tuple[Source, Any, int]:
    """Run ``consume`` on a source, re-planning until the types hold every value:
    (source, result, restarts).  Nothing internal escapes: a value that fits no
    plan ends as a DataError."""
    restarts = 0
    full = o.infer == "full"
    while True:
        source: Optional[Source] = None
        try:
            if dialect is not None:
                source = CsvSource(src, src_fmt, dialect, o, text_out)
                source.infer(full)
            else:
                source = JsonSource(src, src_fmt, o, full)
            return source, consume(source), restarts
        except _Restart as r:
            restarts += 1
            names = source.names if source is not None else []
            if restarts > len(names) + 8:
                raise DataError("could not settle on a schema") from None
            if r.reason == "encoding":
                if dialect is None or dialect.encoding not in ("utf-8", "utf8") or source is None:
                    raise DataError("input is not valid in any supported encoding") from None
                warnings.append("input is not valid UTF-8; re-read as windows-1252")
                dialect.encoding = "cp1252"
            elif r.reason == "columns":
                if full:
                    raise DataError(f"field {r.value!r} appeared after the full scan") from None
                warnings.append(f"field '{r.value}' first appears beyond the sample; re-planned from a full scan")
                full = True
            elif not full:
                name = names[r.column or 0]
                warnings.append(f"column '{name}': value '{r.value}' beyond the sample does not fit "
                                f"{source.types[r.column or 0].label}; re-planned from a full scan")  # type: ignore[union-attr]
                full = True
            else:
                name = names[r.column or 0]
                if name in o.schema or dialect is None:
                    raise DataError(f"column '{name}': value '{r.value}' cannot be stored as "
                                    f"{o.schema.get(name) or source.types[r.column or 0].label}") from None  # type: ignore[union-attr]
                o.schema = dict(o.schema, **{name: "string"})
                warnings.append(f"column '{name}': value '{r.value}' forced the column to text")


def convert(src: Path, dst: Path, src_fmt: str, dst_fmt: str, dst_comp: Optional[str], o: ConvertOptions, progress: Progress) -> Report:
    started = time.perf_counter()
    bytes_in = src.stat().st_size
    text_out = dst_fmt in ("csv", "tsv", "psv", "dcz")
    report = Report(source=str(src), destination=str(dst), source_format=src_fmt, destination_format=dst_fmt,
                    engine="pure", bytes_in=bytes_in, threads=1)
    if src_fmt in ("csv", "tsv", "psv") and dst_fmt == "dcz" and not o.columns and not o.limit:
        return compress(src, dst, o.profile, o.overwrite, progress, transparent=True)
    if src_fmt not in DELIMITED and src_fmt not in ("json", "jsonl"):
        raise FormatError(f"the pure engine cannot read {src_fmt}")
    delimited = src_fmt in DELIMITED
    quarantine = Quarantine(src, dst, o, delimited)
    dialect = _dialect(src, src_fmt, o) if delimited else None
    warnings: List[str] = []

    def write(source: Source) -> int:
        with atomic_output(dst, overwrite=o.overwrite) as tmp, _io.open_output(tmp, dst_comp, o.level) as out:
            sink = Sink(out, dst_fmt, source.names, o)
            rows = 0
            limit = o.limit if o.limit is not None else -1
            last = 0.0
            for row in source.rows():
                if rows == limit:
                    break
                sink.write(row)
                rows += 1
                if progress is not None and rows % 1024 == 0:
                    now = time.perf_counter()
                    if now - last > 0.1:
                        last = now
                        if progress(source.consumed(), bytes_in) is False:
                            raise KeyboardInterrupt
            sink.close()
            if progress is not None and progress(bytes_in, bytes_in) is False:
                raise KeyboardInterrupt
        return rows

    source, rows, restarts = plan(src, src_fmt, o, dialect, text_out, write, warnings)
    bad = source.bad
    if isinstance(source, CsvSource) and source.forced_at is not None:
        warnings.append(_csvtok.forced_warning(source.forced_at))
    quarantine.finish(source.names, dialect.delimiter if dialect else ",", bad, report, warnings)
    report.rows = rows
    report.columns = len(source.names)
    report.schema = [(n, t.label) for n, t in zip(source.names, source.types)]
    report.bad_rows = len(bad)
    report.blank_lines = source.blank
    report.irregular_records = source.irregular
    report.restarts = restarts
    report.warnings = source.warnings + warnings
    report.dialect = dialect.to_dict() if dialect else None
    report.bytes_out = dst.stat().st_size
    report.seconds = time.perf_counter() - started
    report.compression = dst_fmt if not dst_comp else f"{dst_fmt}/{dst_comp}"
    return report


def compress(src: Path, dst: Path, profile: str, overwrite: bool, progress: Progress, block_size: Optional[int] = None,
             transparent: bool = False) -> Report:
    """DCZ of the file's own bytes (``transparent``: of the decompressed content, which
    converting a .csv.gz to DCZ wants)."""
    started = time.perf_counter()
    size = src.stat().st_size

    def open_src() -> IO[bytes]:
        return _io.open_binary(src) if transparent else open(src, "rb")

    with open_src() as f:
        head = f.read(1 << 20)
    csvmode: Optional[Tuple[str, Optional[str]]] = None
    escape: Optional[str] = None
    if head and b"\x00" not in head:
        d = sniff_bytes(head, len(head) < (1 << 20))
        if d.encoding in ("utf-8", "cp1252"):
            csvmode = (d.delimiter, d.quote)
            escape = d.escape
    with atomic_output(dst, overwrite=overwrite) as tmp, open_src() as f:
        dcz.write_file(f, tmp, csvmode, "fast" if profile == "fast" else profile, block_size or (4 << 20), progress, size,
                       escape=escape)
    return Report(source=str(src), destination=str(dst), source_format="csv" if csvmode else "bytes",
                  destination_format="dcz", engine="pure", bytes_in=size, bytes_out=dst.stat().st_size,
                  seconds=time.perf_counter() - started, threads=1,
                  compression=f"dcz/{'csv' if csvmode else 'raw'}/{profile}")


def decompress(src: Path, dst: Path, overwrite: bool, progress: Progress) -> Report:
    started = time.perf_counter()
    footer = dcz.read_footer(src)
    total = footer["original_size"]
    done = 0
    with atomic_output(dst, overwrite=overwrite) as tmp, open(tmp, "wb") as out:
        for block in dcz.iter_decoded(src):
            out.write(block)
            done += len(block)
            if progress is not None:
                progress(done, total)
    return Report(source=str(src), destination=str(dst), source_format="dcz",
                  destination_format="csv" if footer["kind"] == "csv" else "bytes", engine="pure",
                  bytes_in=src.stat().st_size, bytes_out=dst.stat().st_size,
                  seconds=time.perf_counter() - started, threads=1,
                  compression=f"dcz/{footer['kind']}/{footer.get('profile', '')}")


def head(path: Path, n: int, fmt: str, o: ConvertOptions) -> Dict[str, Any]:
    if fmt not in DELIMITED and fmt not in ("jsonl", "json"):
        raise FormatError(f"the pure engine cannot read {fmt}")
    dialect = _dialect(path, fmt, o) if fmt in DELIMITED else None
    source, rows, _ = plan(path, fmt, o, dialect, False, lambda s: list(islice(s.rows(), max(0, n))), [])
    return {"columns": source.names, "types": [t.label for t in source.types], "rows": [[csv_text(v) for v in r] for r in rows]}


def count(path: Path, fmt: str, o: ConvertOptions) -> int:
    if fmt in DELIMITED:
        dialect = _dialect(path, fmt, o)
        try:
            return CsvSource(path, fmt, dialect, o, text_output=True).count()
        except _Restart:
            if dialect.encoding not in ("utf-8", "utf8"):
                raise DataError("input is not valid in any supported encoding") from None
            dialect.encoding = "cp1252"
            return CsvSource(path, fmt, dialect, o, text_output=True).count()
    if fmt in ("jsonl", "json"):
        src = JsonSource.__new__(JsonSource)
        src.path, src.fmt, src.o = path, fmt, o
        return sum(1 for _ in src._objects())
    raise FormatError(f"the pure engine cannot read {fmt}")
