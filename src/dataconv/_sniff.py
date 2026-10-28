"""CSV dialect detection (the Python twin of the native sniffer).

Detects the encoding, delimiter, quoting, line terminator, header row and
leading preamble lines from a sample of the file.
"""

from __future__ import annotations

import codecs
import csv
import os
import re
from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from . import _compat, _csvtok, _io, infer  # _io also registers the cp1252 error handler
from .errors import OptionsError

CANDIDATES = [",", "\t", ";", "|", "^", "~", ":"]
#: a time of day: H:MM or HH:MM:SS (a leap second too) with up to 9 fraction digits,
#: then optionally Z or a +HH:MM / -HH:MM offset
_TIME = re.compile(r"([0-9]{1,2}):([0-9]{2})(?::([0-9]{2})(?:\.[0-9]{1,9})?)?(?:Z|[+-][0-9]{2}:[0-9]{2})?")
#: characters that may delimit a sample read as one column, the first absent one wins
_LONE = (",", "\t", ";", "|", "\x1f")


@dataclass
class Dialect:
    delimiter: str = ","
    quote: Optional[str] = '"'
    escape: Optional[str] = None
    double_quote: bool = True
    terminator: str = "\n"
    encoding: str = "utf-8"
    bom: bool = False
    header: bool = True
    skip_rows: int = 0
    columns: List[str] = field(default_factory=list)
    has_quotes: bool = False
    max_record_len: int = 0
    sample_records: int = 0
    confidence: float = 1.0

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Dialect:
        known = {k: d[k] for k in cls.__dataclass_fields__ if k in d}
        return cls(**known)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @property
    def leading_records(self) -> int:
        return self.skip_rows + (1 if self.header else 0)

    def csv_kwargs(self) -> Dict[str, Any]:
        kw: Dict[str, Any] = {"delimiter": self.delimiter, "doublequote": self.double_quote, "strict": False}
        if self.quote:
            kw["quotechar"] = self.quote
        else:
            kw["quoting"] = csv.QUOTE_NONE
        if self.escape:
            kw["escapechar"] = self.escape
        return kw


def parse_char_option(value: Optional[str]) -> Optional[str]:
    """A one-character dialect option (``;``, ``tab``, ``\\t``, ``space``, ``none``), as
    the native engine reads it: '' for none, else a single ASCII character."""
    if value is None:
        return None
    named = {"\\t": "\t", "tab": "\t", "TAB": "\t", "\\n": "\n", "space": " ", "none": "", "": ""}
    if value in named:
        return named[value]
    if len(value) != 1 or not value.isascii():
        raise OptionsError(f"{value!r} is not a single ASCII character (e.g. ',', ';', '|', 'tab' or 'none')")
    return value


def detect_encoding(sample: bytes, complete: bool = True) -> Tuple[str, bool]:
    """(Python codec name, has BOM)."""
    if sample.startswith(b"\xef\xbb\xbf"):
        return "utf-8", True
    if sample.startswith(b"\xff\xfe"):
        return "utf-16-le", True
    if sample.startswith(b"\xfe\xff"):
        return "utf-16-be", True
    if len(sample) >= 64:
        pairs = sample[: len(sample) // 2 * 2]
        odd = sum(1 for i in range(0, len(pairs), 2) if pairs[i + 1] == 0 and pairs[i] != 0)
        even = sum(1 for i in range(0, len(pairs), 2) if pairs[i] == 0 and pairs[i + 1] != 0)
        if odd * 10 > len(pairs) // 2 * 7:
            return "utf-16-le", False
        if even * 10 > len(pairs) // 2 * 7:
            return "utf-16-be", False
    check = sample if complete else trim_partial_utf8(sample)
    try:
        check.decode("utf-8")
        return "utf-8", False
    except UnicodeDecodeError:
        pass
    detector = _compat.optional("charset_normalizer")
    if detector is not None:
        try:
            best = detector.from_bytes(sample).best()
            if best is not None and best.encoding:
                return str(best.encoding), False
        except Exception:
            pass
    return "cp1252", False


def trim_partial_utf8(sample: bytes) -> bytes:
    n = len(sample)
    for back in range(1, min(3, n) + 1):
        b = sample[n - back]
        if b & 0xC0 == 0xC0:
            needed = 4 if b >= 0xF0 else 3 if b >= 0xE0 else 2
            return sample[: n - back] if back < needed else sample
        if b & 0x80 == 0:
            return sample
    return sample


def decode_sample(raw: bytes, encoding: str, complete: bool) -> str:
    encoding = _io.python_codec(encoding)
    utf8 = codecs.lookup(encoding).name == "utf-8"
    data = raw if complete or not utf8 else trim_partial_utf8(raw)
    errors = "dataconv-c1" if _io.text_errors(encoding) == "dataconv-c1" else "replace"
    return data.decode(encoding, errors=errors).lstrip("﻿")


def _sample(
    text: str, delimiter: str, quote: Optional[str], escape: Optional[str], limit: int, complete: bool
) -> Tuple[List[List[str]], List[int], int]:
    """Records of the sample, split exactly as the converters split them (the record
    cut off by the end of an incomplete sample is dropped)."""
    d = _csvtok.Dialect(delimiter, quote, escape, escape is None)
    return _csvtok.sample_records(text, d, limit, complete)


def _rows(text: str, delimiter: str, quote: Optional[str], escape: Optional[str], limit: int, complete: bool) -> List[List[str]]:
    return _sample(text, delimiter, quote, escape, limit, complete)[0]


def _backslash_escape(text: str, quote: Optional[str]) -> Optional[str]:
    """Backslash-escaped quotes (``\\"``) and no RFC 4180 doubled quotes mean the
    writer escapes with backslashes (the native rule)."""
    if not quote or quote not in text:
        return None
    backslashed = doubled = 0
    i = 1
    n = len(text)
    while i < n:
        i = text.find(quote, i)
        if i < 0:
            break
        if text[i - 1] == "\\":
            backslashed += 1
        elif i + 1 < n and text[i + 1] == quote and text[i - 1] != quote:
            doubled += 1
            i += 1
        i += 1
    return "\\" if backslashed > 0 and doubled == 0 else None


def _time_of_day(v: str) -> bool:
    m = _TIME.fullmatch(v)
    return bool(m) and int(m.group(1)) <= 23 and int(m.group(2)) <= 59 and int(m.group(3) or 0) <= 60


def _one_temporal_column(text: str, quote: Optional[str], escape: Optional[str], complete: bool) -> bool:
    """Whether a sample that ``:`` would split is really one column of times, dates or
    timestamps (``10:30``, ``2024-01-02 03:04:05``)."""
    rows = _rows(text, ",", quote, escape, 400, complete)
    if any(len(r) != 1 for r in rows):
        return False
    values = [r[0] for r in rows[1:] if r[0] != ""]  # the first row may be a header
    if not values:
        return False
    hits = sum(1 for v in values if infer.parse_timestamp(v) is not None or infer.parse_date(v) is not None or _time_of_day(v))
    return hits * 10 >= len(values) * 9


def _score(rows: Sequence[Sequence[str]]) -> Tuple[float, int]:
    counts = [len(r) for r in rows if not (len(r) <= 1 and (not r or r[0] == ""))]
    if not counts:
        return 0.0, 1
    freq = Counter(counts)
    best = max(freq.values())
    mode = next(c for c in counts if freq[c] == best)  # ties: earliest
    if mode < 2:
        return 0.0, mode
    return best / len(counts) * (1 + min(mode, 64) / 200), mode


def _kind(v: str) -> str:
    if v == "":
        return "empty"
    if infer.parse_int(v, True)[0] != "no" or infer.parse_float(v, True) is not None:
        return "number"
    if v in ("true", "True", "TRUE", "false", "False", "FALSE"):
        return "bool"
    if infer.parse_date(v) is not None or infer.parse_timestamp(v) is not None:
        return "temporal"
    return "text"


def looks_like_header(rows: Sequence[Sequence[str]]) -> bool:
    if not rows:
        return True
    first, rest = rows[0], rows[1:101]
    if not rest:
        return True
    score = 0
    for j, name in enumerate(first):
        col = [r[j] for r in rest if j < len(r) and r[j] != ""]
        if not col:
            continue
        k = _kind(name)
        kinds = [_kind(v) for v in col]
        typed = all(x != "text" for x in kinds)
        if k == "text" and typed:
            score += 3
        elif k == "number" and all(x == "number" for x in kinds):
            score -= 3
        elif k not in ("text", "empty") and typed:
            score -= 2
        elif name in col:
            score -= 1
    non_empty = [n for n in first if n]
    if len(set(non_empty)) != len(non_empty):
        score -= 2
    return score >= 0


def _layout(
    text: str, delimiter: str, quote: Optional[str], escape: Optional[str], skip_rows: Optional[int], complete: bool
) -> Tuple[List[List[str]], List[int], int, int, int]:
    """(rows, terminator counts, longest record, dominant field count, preamble rows
    before the data, ``skip_rows`` when given) of the sample read with ``delimiter``."""
    rows, terms, longest = _sample(text, delimiter, quote, escape, 2000, complete)
    # dominant field count from the body of the sample (so preamble lines cannot
    # outvote the data); small samples use every row
    body = rows[len(rows) // 4 :] if len(rows) >= 20 else rows
    _, ncols = _score(body)
    if skip_rows is not None:
        return rows, terms, longest, ncols, skip_rows
    k = 0
    while k < len(rows) and k < 50:
        n = len(rows[k])
        blank = n == 0 or (n == 1 and rows[k][0] == "")
        if n == ncols and not blank:
            break
        if not (blank or (ncols >= 2 and n <= ncols // 2)):
            k = 0
            break
        k += 1
    return rows, terms, longest, ncols, (0 if k >= len(rows) else k)


def _header_evidence(rows: Sequence[Sequence[str]]) -> bool:
    """Whether the first of ``rows`` is a header with evidence: a text name over a
    column whose values are all numbers, dates, timestamps or booleans.  (All-text data
    is read as having a header when undecided; this asks for more.)"""
    if not rows:
        return False
    first, rest = rows[0], rows[1:101]
    for j, name in enumerate(first):
        col = [r[j] for r in rest if j < len(r) and r[j] != ""]
        if _kind(name) == "text" and col and all(_kind(v) != "text" for v in col):
            return True
    return False


def unique_names(raw: Sequence[str], width: int) -> List[str]:
    """Column names exactly as written; only an empty or blank name becomes
    ``column_N``, and duplicates get ``_2``, ``_3``..."""
    out: List[str] = []
    seen = set()
    for i in range(width):
        text = raw[i] if i < len(raw) else ""
        base = text if text.strip() else f"column_{i + 1}"
        name, n = base, 2
        while name in seen:
            name, n = f"{base}_{n}", n + 1
        seen.add(name)
        out.append(name)
    return out


def sniff_bytes(
    raw: bytes,
    complete: bool,
    delimiter: Optional[str] = None,
    quote: Optional[str] = None,
    escape: Optional[str] = None,
    encoding: Optional[str] = None,
    header: Optional[bool] = None,
    skip_rows: Optional[int] = None,
    extension: Optional[str] = None,
) -> Dialect:
    detected, bom = detect_encoding(raw, complete)
    if encoding is not None and not _io.known_encoding(encoding):
        raise OptionsError(f"unknown encoding '{encoding}'")
    enc = encoding or detected
    text = decode_sample(raw, enc, complete)
    q = '"' if quote is None else (parse_char_option(quote) or None)
    has_quotes = bool(q) and q in text
    esc = (parse_char_option(escape) or None) if escape is not None else _backslash_escape(text, q)
    tsv_hint = (extension or "").lower().lstrip(".") in ("tsv", "tab")

    def lone() -> Optional[str]:
        # a character that never occurs in the sample, for reading it as one column
        return next((c for c in (_LONE if complete else _LONE[-1:]) if c not in text), None)

    confidence = 1.0
    if delimiter:
        delim = parse_char_option(delimiter) or ","
    else:
        best = (",", 0.0)
        for cand in CANDIDATES:
            score, _ = _score(_rows(text, cand, q, esc, 400, complete))
            if cand == "\t" and tsv_hint:
                score *= 1.25
            if score > best[1] + 1e-9:
                best = (cand, score)
        delim, confidence = best if best[1] > 0 else ("\t" if tsv_hint else ",", 0.0)
        if delim == ":" and _one_temporal_column(text, q, esc, complete):
            # "10:30" or "2024-01-02 03:04:05" splits on ':' consistently, but it is a
            # single column of times
            delim = lone() or ","
    if delim in (q, esc):
        raise OptionsError(f"the delimiter {delim!r} is also the quote or escape character; they must differ")
    rows, terms, longest, ncols, skip = _layout(text, delim, q, esc, skip_rows, complete)
    # A first row without the delimiter, taken for a one-line preamble before data that
    # has no header, is far more often the header of a single column whose values
    # contain that character (MAC addresses, URLs with ports, key:value pairs).  Unless
    # the rows below show a real header (a name over typed values), read one column:
    # every line is kept whole either way.
    if (
        delimiter is None
        and skip_rows is None
        and skip == 1
        and rows
        and len(rows[0]) == 1
        and rows[0][0] != ""
        and delim not in rows[0][0]
        and not _header_evidence(rows[1:])
    ):
        c = lone()
        if c is not None:
            delim = c
            rows, terms, longest, ncols, skip = _layout(text, delim, q, esc, None, complete)
    lf, crlf, cr = terms[_csvtok.LF], terms[_csvtok.CRLF], terms[_csvtok.CR]
    terminator = "\r\n" if crlf >= lf and crlf >= cr and crlf > 0 else "\r" if cr > lf and cr > crlf else "\n"
    data_rows = [r for r in rows[skip:]]
    has_header = looks_like_header(data_rows) if header is None else header
    width = max(len(data_rows[0]) if data_rows else ncols, ncols, 1)
    columns = unique_names(data_rows[0], width) if has_header and data_rows else [f"column_{i + 1}" for i in range(width)]
    max_len = longest
    return Dialect(
        delimiter=delim,
        quote=q,
        escape=esc,
        double_quote=esc is None,
        terminator=terminator,
        encoding=enc,
        bom=bom,
        header=has_header,
        skip_rows=skip,
        columns=columns,
        has_quotes=has_quotes,
        max_record_len=max_len,
        sample_records=len(rows),
        confidence=confidence,
    )


def sniff(path: os.PathLike[str] | str, **options: Any) -> Dialect:
    """Sniff a CSV file (transparently decompressing it)."""
    from ._io import open_binary
    from .formats import split_name

    size = 2 << 20
    with open_binary(path) as f:
        raw = f.read(size)
    complete = len(raw) < size
    ext = options.pop("extension", None)
    if ext is None:
        _, fmt, _ = split_name(path)
        ext = fmt
    return sniff_bytes(raw, complete, extension=ext, **options)
