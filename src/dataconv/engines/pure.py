"""Standard-library engine: CSV/TSV/PSV, JSON Lines, JSON arrays and DCZ.

Slower than the native and arrow engines, but it runs on any Python 3.8+ and
applies exactly the same lossless type rules and output formatting.
"""

from __future__ import annotations

import csv
import io
import json
import math
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import IO, Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple

from .. import _io, dcz, formats, infer
from .._sniff import Dialect, sniff, sniff_bytes
from .._util import atomic_output
from ..errors import DataError, FormatError
from ..options import ConvertOptions
from ..report import Report

Progress = Optional[Callable[[int, int], Any]]
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))
QUOTE_NOTNULL = getattr(csv, "QUOTE_NOTNULL", None)  # Python 3.12+: tells "" from a missing value
SAMPLE_ROWS = 100_000


class _Restart(Exception):
    def __init__(self, reason: str, column: Optional[int] = None, value: str = "") -> None:
        super().__init__(reason)
        self.reason = reason
        self.column = column
        self.value = value


# ---------------------------------------------------------------------------------
# Value formatting (identical to the native writers)
# ---------------------------------------------------------------------------------


def _float_text(v: float) -> str:
    if v != v:
        return "NaN"
    if v in (math.inf, -math.inf):
        return "inf" if v > 0 else "-inf"
    return repr(v)


def csv_text(v: Any) -> Optional[str]:
    """Text of a value in CSV; None means "write an empty unquoted field"."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        return _float_text(v)
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False, separators=(",", ":"))
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
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
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return "null" if v != v or v in (math.inf, -math.inf) else repr(v)
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, (datetime, date)):
        return json.dumps(v.isoformat())
    if isinstance(v, bytes):
        return json.dumps(v.decode("utf-8", errors="replace"))
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False, separators=(",", ":"), default=str)
    return json.dumps(v, ensure_ascii=False)


def json_object(names: Sequence[str], values: Sequence[Any]) -> str:
    return "{" + ",".join(json.dumps(n, ensure_ascii=False) + ":" + json_value(v) for n, v in zip(names, values)) + "}"


# ---------------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------------


class Source:
    names: List[str]
    types: List[infer.LogicalType]

    def rows(self) -> Iterator[List[Any]]:
        raise NotImplementedError

    def consumed(self) -> int:
        return 0


def _open_csv_text(path: Path, fmt: str, encoding: str) -> IO[str]:
    if fmt == "dcz":
        enc = "utf-8-sig" if encoding.lower() in ("utf-8", "utf8") else encoding
        return io.TextIOWrapper(dcz.DczReader(path), encoding=enc, errors=_io.text_errors(enc), newline="")  # type: ignore[arg-type]
    return _io.open_text(path, encoding)


class CsvSource(Source):
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
        self.mode = "string" if text_output and not o.schema else o.types
        self.types = [infer.STRING] * len(self.names)
        self.bad: List[List[str]] = []
        self.blank = 0
        self.warnings: List[str] = []
        self._file: Optional[IO[str]] = None

    def raw_rows(self) -> Iterator[List[Optional[str]]]:
        f = _open_csv_text(self.path, self.fmt, self.dialect.encoding)
        self._file = f
        kw = self.dialect.csv_kwargs()
        if QUOTE_NOTNULL is not None and self.dialect.quote:
            kw["quoting"] = QUOTE_NOTNULL
        try:
            reader = csv.reader(f, **kw)
            skip = self.dialect.leading_records
            for row in reader:
                if skip:
                    skip -= 1
                    continue
                yield row
        except UnicodeDecodeError as exc:
            raise _Restart("encoding") from exc
        finally:
            f.close()

    def consumed(self) -> int:
        f = self._file
        try:
            raw = getattr(f, "buffer", None)
            return int(raw.tell()) if raw is not None and not raw.closed else 0
        except (OSError, ValueError, AttributeError):
            return 0

    def _fields(self, row: List[Optional[str]]) -> Optional[List[Optional[str]]]:
        if not row or (len(row) == 1 and row[0] in ("", None)):
            self.blank += 1
            return None
        if len(row) != self.ncols:
            if self.o.bad_lines == "pad" and len(row) < self.ncols:
                row = row + [None] * (self.ncols - len(row))
            elif self.o.bad_lines == "error":
                raise DataError(f"record with {len(row)} fields where {self.ncols} were expected: {row[:8]}")
            else:
                self.bad.append([x or "" for x in row])
                return None
        return [row[i] for i in self.projection]

    def infer(self, full: bool) -> None:
        if self.mode == "string" and not self.o.schema:
            return
        stats = [infer.ColumnStats() for _ in self.names]
        nulls = self.o.null_values
        for n, row in enumerate(self.raw_rows()):
            if not full and n >= SAMPLE_ROWS:
                break
            fields = self._fields(row)
            if fields is None:
                continue
            for s, v in zip(stats, fields):
                s.observe("" if v is None else v, self.mode, nulls)
        self.bad.clear()
        self.blank = 0
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
            fields = self._fields(row)
            if fields is None:
                continue
            out = []
            for j, (conv, v) in enumerate(zip(convs, fields)):
                try:
                    out.append(conv("" if v is None else v, v is None or (v == "" and QUOTE_NOTNULL is None)))
                except (ValueError, KeyError):
                    raise _Restart("violation", j, v or "") from None
            yield out


def _json_type(values: Sequence[Any]) -> infer.LogicalType:
    """Lossless type for JSON values: big integers become exact decimals, mixed or
    nested values stay JSON."""
    kinds = {type(v) for v in values if v is not None}
    if not kinds:
        return infer.STRING
    if kinds == {bool}:
        return infer.LogicalType("bool")
    if kinds <= {int}:
        ints = [v for v in values if v is not None]
        if min(ints) >= infer.INT64_MIN and max(ints) <= infer.INT64_MAX:
            return infer.LogicalType("int64")
        digits = max(len(str(abs(v))) for v in ints)
        return infer.LogicalType("decimal", digits, 0) if digits <= 38 else infer.STRING
    if kinds <= {int, float}:
        exact = all(isinstance(v, float) or abs(v) <= (1 << 53) for v in values if v is not None)
        return infer.LogicalType("float64") if exact else infer.LogicalType("json")
    if kinds == {str}:
        return infer.STRING
    return infer.LogicalType("json")


class JsonSource(Source):
    """JSON Lines or a JSON array of objects (streamed element by element)."""

    def __init__(self, path: Path, fmt: str, o: ConvertOptions) -> None:
        self.path, self.fmt, self.o = path, fmt, o
        self.bad: List[List[str]] = []
        self.blank = 0
        self.warnings: List[str] = []
        sample = []
        names: Dict[str, None] = {}
        limit = None if o.infer == "full" else 100_000
        for n, obj in enumerate(self._objects()):
            sample.append(obj)
            for k in obj:
                names.setdefault(k, None)
            if limit is not None and n >= limit:
                break
        self.names = o.columns or list(names)
        self.types = [_json_type([r.get(k) for r in sample]) for k in self.names]

    def _objects(self) -> Iterator[Dict[str, Any]]:
        with _io.open_text(self.path, "utf-8") as f:
            if self.fmt == "jsonl":
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    yield obj if isinstance(obj, dict) else {"value": obj}
                return
            dec = json.JSONDecoder()
            buf = f.read(1 << 20).lstrip()
            if not buf.startswith("["):
                raise FormatError(f"{self.path}: expected a JSON array of records")
            pos = 1
            while True:
                while True:
                    while pos < len(buf) and buf[pos] in " \t\r\n,":
                        pos += 1
                    if pos < len(buf):
                        break
                    more = f.read(1 << 20)
                    if not more:
                        return
                    buf, pos = buf[pos:] + more, 0
                if buf[pos] == "]":
                    return
                while True:
                    try:
                        obj, end = dec.raw_decode(buf, pos)
                        break
                    except json.JSONDecodeError:
                        more = f.read(1 << 20)
                        if not more:
                            raise
                        buf, pos = buf[pos:] + more, 0
                yield obj if isinstance(obj, dict) else {"value": obj}
                pos = end
                if pos > (1 << 20):
                    buf, pos = buf[pos:], 0

    def rows(self) -> Iterator[List[Any]]:
        for obj in self._objects():
            yield [obj.get(k) for k in self.names]


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
            self._emit(json_object(self.names, row) + "\n")
        elif self.fmt == "json":
            self._emit(("\n" if self.first else ",\n") + json_object(self.names, row))
            self.first = False
        else:
            raise FormatError(f"the pure engine cannot write {self.fmt}")

    def close(self) -> None:
        if self.fmt == "json":
            self._emit("\n]\n" if not self.first else "]\n")
        self.flush()
        if self.dcz is not None:
            self.dcz.close()


def _dialect(path: Path, fmt: str, o: ConvertOptions) -> Dialect:
    if fmt == "dcz":
        with dcz.DczReader(path) as r:
            raw = r.read(2 << 20)
        return sniff_bytes(raw, len(raw) < (2 << 20), delimiter=o.delimiter, quote=o.quote, escape=o.escape,
                           encoding=o.encoding, header=o.header, skip_rows=o.skip_rows)
    delimiter = o.delimiter or (formats.DELIMITERS[fmt] if fmt in ("tsv", "psv") else None)
    return sniff(path, delimiter=delimiter, quote=o.quote, escape=o.escape, encoding=o.encoding, header=o.header, skip_rows=o.skip_rows)


def convert(src: Path, dst: Path, src_fmt: str, dst_fmt: str, dst_comp: Optional[str], o: ConvertOptions, progress: Progress) -> Report:
    started = time.perf_counter()
    bytes_in = src.stat().st_size
    text_out = dst_fmt in ("csv", "tsv", "psv", "dcz")
    report = Report(source=str(src), destination=str(dst), source_format=src_fmt, destination_format=dst_fmt,
                    engine="pure", bytes_in=bytes_in, threads=1)
    if src_fmt in ("csv", "tsv", "psv") and dst_fmt == "dcz" and not o.columns and not o.limit:
        return compress(src, dst, o.profile, o.overwrite, progress)
    dialect = _dialect(src, src_fmt, o) if src_fmt in ("csv", "tsv", "psv", "dcz") else None
    restarts = 0
    full = o.infer == "full"
    warnings: List[str] = []
    while True:
        source: Source
        if dialect is not None:
            source = CsvSource(src, src_fmt, dialect, o, text_out)
            source.infer(full)
        else:
            source = JsonSource(src, src_fmt, o)
        try:
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
            break
        except _Restart as r:
            restarts += 1
            if restarts > len(source.names) + 4:
                raise DataError("could not settle on a schema") from None
            if r.reason == "encoding":
                if dialect is None or dialect.encoding not in ("utf-8", "utf8"):
                    raise DataError("input is not valid in any supported encoding") from None
                warnings.append("input is not valid UTF-8; re-read as windows-1252")
                dialect.encoding = "cp1252"
            elif not full:
                name = source.names[r.column or 0]
                warnings.append(f"column '{name}': value '{r.value}' beyond the sample does not fit "
                                f"{source.types[r.column or 0].label}; re-planned from a full scan")
                full = True
            else:
                name = source.names[r.column or 0]
                if name in o.schema:
                    raise DataError(f"column '{name}': value '{r.value}' cannot be stored as {o.schema[name]}") from None
                o.schema = dict(o.schema, **{name: "string"})
                warnings.append(f"column '{name}': value '{r.value}' forced the column to text")
    bad = getattr(source, "bad", [])
    if bad and o.bad_lines != "skip":
        qpath = Path(o.quarantine) if o.quarantine else dst.with_name(dst.name + ".rejected.csv")
        with open(qpath, "w", encoding="utf-8", newline="") as qf:
            qf.write(csv_line(source.names, ",", "\n"))
            for row in bad:
                qf.write(csv_line(row, dialect.delimiter if dialect else ",", "\n"))
        report.quarantine = str(qpath)
        warnings.append(f"{len(bad)} malformed rows moved to {qpath}")
    elif bad:
        warnings.append(f"{len(bad)} malformed rows skipped")
    report.rows = rows
    report.columns = len(source.names)
    report.schema = [(n, t.label) for n, t in zip(source.names, source.types)]
    report.bad_rows = len(bad)
    report.blank_lines = getattr(source, "blank", 0)
    report.restarts = restarts
    report.warnings = getattr(source, "warnings", []) + warnings
    report.dialect = dialect.to_dict() if dialect else None
    report.bytes_out = dst.stat().st_size
    report.seconds = time.perf_counter() - started
    report.compression = dst_fmt if not dst_comp else f"{dst_fmt}/{dst_comp}"
    return report


def compress(src: Path, dst: Path, profile: str, overwrite: bool, progress: Progress, block_size: Optional[int] = None) -> Report:
    started = time.perf_counter()
    size = src.stat().st_size
    with _io.open_binary(src) as f:
        head = f.read(1 << 20)
    csvmode: Optional[Tuple[str, Optional[str]]] = None
    if head and b"\x00" not in head:
        d = sniff_bytes(head, len(head) < (1 << 20))
        if d.encoding in ("utf-8", "cp1252"):
            csvmode = (d.delimiter, d.quote)
    with atomic_output(dst, overwrite=overwrite) as tmp, _io.open_binary(src) as f:
        dcz.write_file(f, tmp, csvmode, "fast" if profile == "fast" else profile, block_size or (4 << 20), progress, size)
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
    if fmt in ("csv", "tsv", "psv", "dcz"):
        source: Source = CsvSource(path, fmt, _dialect(path, fmt, o), o, text_output=False)
        source.infer(False)
    elif fmt in ("jsonl", "json"):
        source = JsonSource(path, fmt, o)
    else:
        raise FormatError(f"the pure engine cannot read {fmt}")
    rows = []
    for row in source.rows():
        rows.append([csv_text(v) for v in row])
        if len(rows) >= n:
            break
    return {"columns": source.names, "types": [t.label for t in source.types], "rows": rows}


def count(path: Path, fmt: str, o: ConvertOptions) -> int:
    if fmt in ("csv", "tsv", "psv", "dcz"):
        src = CsvSource(path, fmt, _dialect(path, fmt, o), o, text_output=True)
        return sum(1 for row in src.raw_rows() if row and not (len(row) == 1 and row[0] in ("", None)))
    if fmt in ("jsonl", "json"):
        return sum(1 for _ in JsonSource(path, fmt, o)._objects())
    raise FormatError(f"the pure engine cannot read {fmt}")
