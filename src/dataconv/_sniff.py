"""CSV dialect detection (the Python twin of the native sniffer).

Detects the encoding, delimiter, quoting, line terminator, header row and
leading preamble lines from a sample of the file.
"""

from __future__ import annotations

import csv
import io
import os
from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from . import _compat, _io, infer  # noqa: F401  (_io registers the cp1252 error handler)

CANDIDATES = [",", "\t", ";", "|", "^", "~", ":"]


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


def _option_char(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return {"\\t": "\t", "tab": "\t", "TAB": "\t", "space": " ", "none": "", "": ""}.get(value, value[:1])


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
    data = raw if complete or encoding != "utf-8" else trim_partial_utf8(raw)
    errors = "dataconv-c1" if encoding in ("cp1252", "windows-1252") else "replace"
    return data.decode(encoding, errors=errors).lstrip("﻿")


def _rows(text: str, delimiter: str, quote: Optional[str], escape: Optional[str], limit: int, complete: bool) -> List[List[str]]:
    kw: Dict[str, Any] = {"delimiter": delimiter, "strict": False}
    if quote:
        kw["quotechar"] = quote
    else:
        kw["quoting"] = csv.QUOTE_NONE
    if escape:
        kw["escapechar"] = escape
        kw["doublequote"] = False
    if not complete:
        # drop the record cut off by the sample boundary
        cut = text.rfind("\n")
        text = text[: cut + 1] if cut >= 0 else ""
    rows = []
    try:
        for row in csv.reader(io.StringIO(text, newline=""), **kw):
            rows.append(row)
            if len(rows) >= limit:
                break
    except csv.Error:
        pass
    return rows


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


def unique_names(raw: Sequence[str], width: int) -> List[str]:
    out: List[str] = []
    seen = set()
    for i in range(width):
        base = (raw[i] if i < len(raw) else "").strip() or f"column_{i + 1}"
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
    enc = encoding or detected
    text = decode_sample(raw, enc, complete)
    q = '"' if quote is None else (_option_char(quote) or None)
    has_quotes = bool(q) and q in text
    esc = _option_char(escape) if escape is not None else None
    if escape is None and has_quotes and '\\"' in text:
        doubled = sum(1 for i in range(1, len(text) - 1) if text[i] == '"' and text[i + 1] == '"' and text[i - 1] not in '\\"')
        esc = "\\" if doubled == 0 else None
    tsv_hint = (extension or "").lower().lstrip(".") in ("tsv", "tab")
    confidence = 1.0
    if delimiter:
        delim = _option_char(delimiter) or ","
    else:
        best = (",", 0.0)
        for cand in CANDIDATES:
            score, _ = _score(_rows(text, cand, q, esc, 400, complete))
            if cand == "\t" and tsv_hint:
                score *= 1.25
            if score > best[1] + 1e-9:
                best = (cand, score)
        delim, confidence = best if best[1] > 0 else ("\t" if tsv_hint else ",", 0.0)
    rows = _rows(text, delim, q, esc, 2000, complete)
    crlf = text.count("\r\n")
    lf = text.count("\n") - crlf
    cr = text.count("\r") - crlf
    terminator = "\r\n" if crlf >= lf and crlf >= cr and crlf > 0 else "\r" if cr > lf and cr > crlf else "\n"
    body = rows[len(rows) // 4 :] if len(rows) >= 20 else rows
    _, ncols = _score(body)
    if skip_rows is None:
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
        skip = 0 if k >= len(rows) else k
    else:
        skip = skip_rows
    data_rows = [r for r in rows[skip:]]
    has_header = looks_like_header(data_rows) if header is None else header
    width = max(len(data_rows[0]) if data_rows else ncols, ncols, 1)
    columns = unique_names(data_rows[0], width) if has_header and data_rows else [f"column_{i + 1}" for i in range(width)]
    max_len = max((len(line) for line in text.splitlines()), default=0)
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
