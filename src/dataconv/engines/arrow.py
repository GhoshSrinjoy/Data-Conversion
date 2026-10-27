"""pyarrow engine: Parquet, Arrow IPC/Feather, ORC, CSV, JSON Lines, JSON and Excel.

CSV input is read as text in record-aligned chunks, and every column is cast with
the same lossless guards as the native engine (vectorised with pyarrow.compute).
"""

from __future__ import annotations

import io
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple

from .. import _compat, _io, dcz, infer
from .._sniff import Dialect
from .._util import atomic_output, prefetch
from ..errors import DataError, FormatError
from ..options import ConvertOptions
from ..report import Report
from ..resources import make_plan
from . import pure

Progress = Optional[Callable[[int, int], Any]]

_INT_RE = r"^-?(0|[1-9][0-9]*)$"
_INT_LOOSE_RE = r"^[+-]?[0-9]+$"
_FLOAT_RE = r"^-?((0|[1-9][0-9]*)(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$|^-?([iI][nN][fF]([iI][nN][iI][tT][yY])?|[nN][aA][nN])$"
_FLOAT_LOOSE_RE = r"^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$|^-?([iI][nN][fF]([iI][nN][iI][tT][yY])?|[nN][aA][nN])$"
_PLAIN_DEC_RE = r"^-?[0-9]*(\.[0-9]*)?$"
_DATE_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
_TS_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]{1,9})?)?(Z|[+-][0-9]{2}(:?[0-9]{2})?)?$"
_BOOLS = ["true", "True", "TRUE", "false", "False", "FALSE"]


def _pa() -> Any:
    pa = _compat.require("pyarrow", "the arrow engine", "arrow")
    # submodules only become attributes of the package once imported
    _compat.require("pyarrow.compute", "the arrow engine", "arrow")
    _compat.require("pyarrow.csv", "the arrow engine", "arrow")
    return pa


class Violation(Exception):
    def __init__(self, column: int, value: str) -> None:
        super().__init__(value)
        self.column = column
        self.value = value


def _first_bad(arr: Any, ok: Any) -> str:
    pc = _pa().compute
    bad = pc.filter(arr, pc.invert(pc.fill_null(ok, True)))
    return str(bad[0].as_py()) if len(bad) else ""


def cast_column(arr: Any, ty: infer.LogicalType, mode: str, nulls: Sequence[str], index: int) -> Any:
    """Cast a string array to `ty`, raising Violation instead of losing information."""
    pa = _pa()
    pc = pa.compute
    if ty.kind == "string":
        return arr
    loose = mode == "loose"
    tokens = sorted(set(infer.NULL_TOKENS) | set(nulls) | (set() if ty.kind == "float64" else set(infer.NAN_TOKENS)))
    arr = pc.if_else(pc.is_in(arr, value_set=pa.array(tokens, pa.string())), pa.scalar(None, pa.string()), arr)

    def require(ok: Any) -> None:
        if not pc.all(pc.fill_null(ok, True)).as_py():
            raise Violation(index, _first_bad(arr, ok))

    try:
        if ty.kind == "bool":
            require(pc.is_in(arr, value_set=pa.array(_BOOLS)))
            truthy = pc.is_in(arr, value_set=pa.array(_BOOLS[:3]))
            return pc.if_else(pc.is_null(arr), pa.scalar(None, pa.bool_()), truthy)
        if ty.kind == "int64":
            require(pc.match_substring_regex(arr, _INT_LOOSE_RE if loose else _INT_RE))
            return pc.cast(arr, pa.int64())
        if ty.kind == "float64":
            require(pc.match_substring_regex(arr, _FLOAT_LOOSE_RE if loose else _FLOAT_RE))
            if not loose:
                long = pc.greater_equal(pc.utf8_length(arr), 16)
                if pc.any(pc.fill_null(long, False)).as_py():
                    for v in pc.filter(arr, pc.fill_null(long, False)).to_pylist():
                        info = infer.parse_float(v)
                        if info is None or not info.lossless:
                            raise Violation(index, v)
            return pc.cast(arr, pa.float64())
        if ty.kind == "decimal":
            require(pc.match_substring_regex(arr, _PLAIN_DEC_RE))
            return pc.cast(arr, pa.decimal128(ty.precision, ty.scale))
        if ty.kind == "date":
            require(pc.match_substring_regex(arr, _DATE_RE))
            return pc.cast(arr, pa.date32())
        if ty.kind == "timestamp":
            require(pc.match_substring_regex(arr, _TS_RE))
            # offsets and precision are checked per distinct value (timestamps repeat little,
            # but this stays exact); the cast itself then runs vectorised
            values = [v for v in pc.unique(arr).to_pylist() if v is not None]
            expect = None if ty.tz is None else (0 if ty.tz == "UTC" else (1 if ty.tz[0] == "+" else -1) * (int(ty.tz[1:3]) * 60 + int(ty.tz[4:6])))
            limit = {"s": 0, "ms": 3, "us": 6, "ns": 9}[ty.unit]
            for v in values:
                ts = infer.parse_timestamp(v)
                if ts is None or ts.offset != expect or ts.frac_digits > limit:
                    raise Violation(index, v)
            if ty.tz is not None:
                arr = pc.replace_substring_regex(arr, r"^([0-9-]{10}) ", r"\1T")
            return pc.cast(arr, pa.timestamp(ty.unit, ty.tz))
    except Violation:
        raise
    except pa.ArrowInvalid as exc:
        raise Violation(index, str(exc)[:80]) from None
    raise FormatError(f"unsupported type {ty}")


# ---------------------------------------------------------------------------------
# CSV reading
# ---------------------------------------------------------------------------------


def _chunks(stream: Any, size: int, quote: Optional[str]) -> Iterator[bytes]:
    carry = b""
    while True:
        data = stream.read(size)
        if not data:
            if carry:
                yield carry
            return
        buf = carry + data
        cut = dcz._record_boundary(buf, quote)
        if cut == 0:
            if len(buf) < size * 8:
                carry = buf
                continue
            cut = buf.rfind(b"\n") + 1 or len(buf)
        yield buf[:cut]
        carry = buf[cut:]


class CsvReader:
    """Record-aligned text chunks parsed by pyarrow, every column as strings."""

    def __init__(self, path: Path, fmt: str, dialect: Dialect, o: ConvertOptions, chunk_size: int) -> None:
        self.path, self.fmt, self.dialect, self.o = path, fmt, dialect, o
        self.chunk_size = chunk_size
        self.bad: List[str] = []
        self.blank = 0
        self.consumed = 0
        cols = dialect.columns
        if o.columns:
            missing = [c for c in o.columns if c not in cols]
            if missing:
                raise FormatError(f"no column named {missing[0]!r}")
        self.names = list(o.columns or cols)

    def _open(self) -> Any:
        if self.fmt == "dcz":
            return dcz.DczReader(self.path)
        return _io.open_binary(self.path)

    def tables(self) -> Iterator[Any]:
        if self.o.bad_lines == "pad":
            yield from self._padded_tables()
            return
        yield from self._arrow_tables()

    def _padded_tables(self) -> Iterator[Any]:
        """Python parsing for bad_lines='pad' (pyarrow can only skip short rows)."""
        pa = _pa()
        src = pure.CsvSource(self.path, self.fmt, self.dialect, self.o, text_output=True)
        batch: List[List[Optional[str]]] = []
        yielded = False
        for row in src.raw_rows():
            fields = src._fields(row)
            if fields is None:
                continue
            batch.append(fields)
            if len(batch) >= 65536:
                yield pa.table({n: pa.array([r[j] for r in batch], pa.string()) for j, n in enumerate(self.names)})
                yielded = True
                batch = []
        self.bad.extend(",".join(x or "" for x in r) for r in src.bad)
        if batch or not yielded:  # always at least one (possibly empty) table
            yield pa.table({n: pa.array([r[j] for r in batch], pa.string()) for j, n in enumerate(self.names)})

    def _arrow_tables(self) -> Iterator[Any]:
        pa = _pa()
        pacsv = pa.csv
        d = self.dialect
        enc = d.encoding.lower()
        utf8 = enc in ("utf-8", "utf8", "utf-8-sig")
        parse = pacsv.ParseOptions(
            delimiter=d.delimiter,
            quote_char=d.quote or False,
            double_quote=d.double_quote,
            escape_char=d.escape or False,
            newlines_in_values=d.has_quotes,
            ignore_empty_lines=True,
            invalid_row_handler=self._invalid,
        )
        convert = pacsv.ConvertOptions(
            column_types={c: pa.string() for c in d.columns},
            strings_can_be_null=True,
            null_values=[""],
            quoted_strings_can_be_null=False,
            include_columns=self.names,
        )
        with self._open() as f:
            first = True
            for chunk in _chunks(f, self.chunk_size, d.quote):
                self.consumed += len(chunk)
                if first and chunk.startswith(b"\xef\xbb\xbf"):
                    chunk = chunk[3:]
                if not utf8:
                    chunk = chunk.decode(d.encoding, errors=_io.text_errors(d.encoding)).encode("utf-8")
                read = pacsv.ReadOptions(
                    column_names=d.columns,
                    skip_rows=d.leading_records if first else 0,
                    block_size=max(1 << 20, min(len(chunk) + 1, 64 << 20)),
                )
                first = False
                try:
                    table = pacsv.read_csv(pa.py_buffer(chunk), read_options=read, parse_options=parse, convert_options=convert)
                except pa.ArrowInvalid as exc:
                    if "UTF8" in str(exc) or "utf8" in str(exc).lower():
                        raise UnicodeDecodeError("utf-8", b"", 0, 1, str(exc)) from None
                    raise DataError(str(exc)) from None
                yield table

    def _invalid(self, row: Any) -> str:
        if self.o.bad_lines == "error":
            return "error"
        self.bad.append(row.text)
        return "skip"


def _infer(reader: CsvReader, o: ConvertOptions, mode: str, full: bool) -> Tuple[List[infer.LogicalType], List[str]]:
    stats = [infer.ColumnStats() for _ in reader.names]
    seen = 0
    for table in reader.tables():
        for j, name in enumerate(reader.names):
            col = table.column(name).to_pylist()
            stats[j].observe_many(("" if v is None else v for v in col), mode, o.null_values)
        seen += table.num_rows
        if not full and seen >= pure.SAMPLE_ROWS:
            break
    reader.bad.clear()
    types, notes = [], []
    for name, s in zip(reader.names, stats):
        if name in o.schema:
            types.append(infer.parse_type(o.schema[name]))
        else:
            types.append(s.decide() if mode != "string" else infer.STRING)
            note = s.explain(name)
            if note:
                notes.append(note)
    return types, notes


# ---------------------------------------------------------------------------------
# Readers for typed formats
# ---------------------------------------------------------------------------------


def read_batches(path: Path, fmt: str, o: ConvertOptions, batch_rows: int = 65536) -> Tuple[Any, Iterator[Any]]:
    """(schema, iterator of RecordBatches) for typed formats."""
    pa = _pa()
    cols = o.columns
    if fmt == "parquet":
        pq = _compat.require("pyarrow.parquet", "reading Parquet", "arrow")
        pf = pq.ParquetFile(path)
        schema = pf.schema_arrow if not cols else pa.schema([pf.schema_arrow.field(c) for c in cols])
        return schema, pf.iter_batches(batch_size=batch_rows, columns=cols, use_threads=True)
    if fmt == "ipc":
        source = pa.memory_map(str(path), "r")
        try:
            reader = pa.ipc.open_file(source)
            batches: Iterator[Any] = (reader.get_batch(i) for i in range(reader.num_record_batches))
        except pa.ArrowInvalid:
            reader = pa.ipc.open_stream(source)
            batches = iter(reader)
        schema = reader.schema
        if cols:
            idx = [schema.get_field_index(c) for c in cols]
            return pa.schema([schema.field(i) for i in idx]), (b.select(idx) for b in batches)
        return schema, batches
    if fmt == "orc":
        orc = _compat.require("pyarrow.orc", "reading ORC (pyarrow built with ORC support)", "arrow")
        of = orc.ORCFile(str(path))
        schema = of.schema if not cols else pa.schema([of.schema.field(c) for c in cols])

        def stripes() -> Iterator[Any]:
            for i in range(of.nstripes):
                stripe = of.read_stripe(i, columns=cols)  # a RecordBatch or a Table, by version
                yield from (stripe.to_batches() if hasattr(stripe, "to_batches") else [stripe])

        return schema, stripes()
    if fmt == "jsonl":
        pj = _compat.require("pyarrow.json", "reading JSON Lines", "arrow")
        with _io.open_binary(path) as f:
            table = pj.read_json(io.BytesIO(f.read()))
        if cols:
            table = table.select(cols)
        return table.schema, iter(table.to_batches(batch_rows))
    if fmt in ("json", "xlsx"):
        rows: Iterator[List[Any]]
        if fmt == "json":
            src = pure.JsonSource(path, fmt, o)
            names, rows, types = src.names, src.rows(), src.types
        else:
            names, rows = _xlsx_rows(path)
            if cols:
                idx = [names.index(c) for c in cols]
                names = list(cols)
                rows = ([r[i] for i in idx] for r in rows)
            types = []
        first: List[List[Any]] = []
        for r in rows:
            first.append(r)
            if len(first) >= batch_rows:
                break
        if not types:
            sample = pa.Table.from_pylist([dict(zip(names, r)) for r in first]) if first else None
            fields = [pa.field(n, sample.schema.field(n).type if sample is not None else pa.string()) for n in names]
        else:
            fields = [pa.field(n, _json_arrow_type(pa, t, [r[j] for r in first])) for j, (n, t) in enumerate(zip(names, types))]
        schema = pa.schema(fields)

        def batch(buf: List[List[Any]]) -> Any:
            columns = list(zip(*buf)) if buf else [() for _ in names]
            try:
                arrays = [_json_array(pa, list(c), f.type) for c, f in zip(columns, schema)]
            except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, TypeError, ValueError) as exc:
                raise DataError(f"{path}: a value later in the file does not fit the inferred schema ({exc}); "
                                "retry with infer='full'") from None
            return pa.RecordBatch.from_arrays(arrays, schema=schema)

        def more() -> Iterator[Any]:
            if first:
                yield batch(first)
            buf: List[List[Any]] = []
            for r in rows:
                buf.append(r)
                if len(buf) >= batch_rows:
                    yield batch(buf)
                    buf = []
            if buf:
                yield batch(buf)

        return schema, more()
    raise FormatError(f"the arrow engine cannot read {fmt}")


def _json_arrow_type(pa: Any, ty: infer.LogicalType, sample: Sequence[Any]) -> Any:
    if ty.kind == "json":
        try:
            return pa.array(list(sample)).type if sample else pa.string()
        except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
            return pa.string()
    return ty.arrow()


def _json_array(pa: Any, values: List[Any], typ: Any) -> Any:
    from decimal import Decimal

    if pa.types.is_decimal(typ):
        return pa.array([None if v is None else Decimal(v) for v in values], typ)
    if pa.types.is_string(typ):
        return pa.array([None if v is None else v if isinstance(v, str) else json.dumps(v, ensure_ascii=False) for v in values], typ)
    return pa.array(values, typ)


def _xlsx_rows(path: Path) -> Tuple[List[str], Iterator[List[Any]]]:
    openpyxl = _compat.require("openpyxl", "reading Excel workbooks", "excel")
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.worksheets[0]
    it = ws.iter_rows(values_only=True)
    header = next(it, None) or ()
    from .._sniff import unique_names

    names = unique_names(["" if h is None else str(h) for h in header], len(header))

    def rows() -> Iterator[List[Any]]:
        try:
            for r in it:
                yield list(r) + [None] * (len(names) - len(r))
        finally:
            wb.close()

    return names, rows()


# ---------------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------------


class Writer:
    def __init__(self, path: Path, fmt: str, compression: Optional[str], schema: Any, o: ConvertOptions, sample: Any = None) -> None:
        pa = _pa()
        self.fmt, self.o, self.schema = fmt, o, schema
        self._inner: Any = None
        self._stream: Any = None
        self._text: Optional[pure.Sink] = None
        if fmt == "parquet":
            pq = _compat.require("pyarrow.parquet", "writing Parquet", "arrow")
            from ..tune import parquet_writer_options

            self._inner = pq.ParquetWriter(str(path), schema, **parquet_writer_options(o, schema, sample))
            self.row_group = o.row_group_rows or 1 << 20
        elif fmt == "ipc":
            comp = {"none": None, "uncompressed": None}.get((o.compression or "zstd").lower(), (o.compression or "zstd").lower())
            options = pa.ipc.IpcWriteOptions(compression=comp)
            # an explicit sink: a writer opened from a path keeps the file open on Windows
            self._stream = pa.OSFile(str(path), "wb")
            self._inner = pa.ipc.new_file(self._stream, schema, options=options)
        elif fmt == "orc":
            orc = _compat.require("pyarrow.orc", "writing ORC (pyarrow built with ORC support)", "arrow")
            self._inner = orc.ORCWriter(str(path), compression=(o.compression or "zstd").upper() if o.compression != "none" else "UNCOMPRESSED")
        elif fmt in ("csv", "tsv", "psv", "jsonl", "json", "dcz"):
            self._stream = _io.open_output(path, compression, o.level)
            self._text = pure.Sink(self._stream, fmt, list(schema.names), o)
        else:
            raise FormatError(f"the arrow engine cannot write {fmt}")

    def write(self, batch: Any) -> None:
        if self._text is not None:
            for row in zip(*(col.to_pylist() for col in batch.columns)):
                self._text.write(row)
        elif self.fmt == "parquet":
            self._inner.write_batch(batch, row_group_size=self.row_group)
        elif self.fmt == "orc":
            self._inner.write(_pa().Table.from_batches([batch]))
        else:
            self._inner.write_batch(batch)

    def abort(self) -> None:
        """Close everything after a failure, ignoring errors."""
        for obj in (self._inner, self._stream):
            if obj is not None:
                try:
                    obj.close()
                except Exception:
                    pass

    def close(self) -> None:
        if self._text is not None:
            self._text.close()
            self._stream.close()
        else:
            self._inner.close()
            if self._stream is not None:
                self._stream.close()


# ---------------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------------


def _dialect(path: Path, fmt: str, o: ConvertOptions) -> Dialect:
    return pure._dialect(path, fmt, o)


def _cast_batches(reader: CsvReader, names: List[str], types: List[infer.LogicalType], schema: Any, mode: str,
                  nulls: Sequence[str], pool: ThreadPoolExecutor) -> Iterator[Any]:
    """Text tables from `reader`, every column cast in parallel (pyarrow releases the GIL)."""
    pa = _pa()
    for table in prefetch(reader.tables(), 1):
        cols = list(pool.map(
            lambda j, t=table: cast_column(t.column(names[j]).combine_chunks(), types[j], mode, nulls, j),
            range(len(names)),
        ))
        yield pa.RecordBatch.from_arrays(cols, schema=schema)


def convert(src: Path, dst: Path, src_fmt: str, dst_fmt: str, dst_comp: Optional[str], o: ConvertOptions, progress: Progress) -> Report:
    pa = _pa()
    started = time.perf_counter()
    bytes_in = src.stat().st_size
    plan = make_plan(bytes_in, o.threads, o.memory)
    if o.threads:
        pa.set_cpu_count(o.threads)
    report = Report(source=str(src), destination=str(dst), source_format=src_fmt, destination_format=dst_fmt,
                    engine="arrow", bytes_in=bytes_in, threads=plan.threads, memory_budget=plan.memory_budget,
                    chunk_size=plan.chunk_size)
    if src_fmt in ("csv", "tsv", "psv") and dst_fmt == "dcz" and not o.columns and not o.limit:
        return pure.compress(src, dst, o.profile, o.overwrite, progress)
    warnings: List[str] = []
    restarts = 0
    is_csv = src_fmt in ("csv", "tsv", "psv", "dcz")
    dialect = _dialect(src, src_fmt, o) if is_csv else None
    text_out = dst_fmt in ("csv", "tsv", "psv", "dcz")
    mode = "string" if (text_out and not o.schema) else o.types
    full = o.infer == "full"
    types: List[infer.LogicalType] = []
    chunk = max(4 << 20, min(plan.chunk_size * 8, 128 << 20))
    while True:
        reader = None
        try:
            if dialect is not None:
                reader = CsvReader(src, src_fmt, dialect, o, chunk)
                types, notes = _infer(reader, o, mode, full)
                names = reader.names
                schema = pa.schema([pa.field(n, t.arrow()) for n, t in zip(names, types)])
                pool = ThreadPoolExecutor(plan.threads)
                stream = _cast_batches(reader, names, types, schema, mode, o.null_values, pool)
            else:
                schema, stream = read_batches(src, src_fmt, o)
                notes = []
                pool = None
            rows = 0
            limit = o.limit
            with atomic_output(dst, overwrite=o.overwrite) as tmp:
                writer: Optional[Writer] = None
                try:
                    for batch in stream:
                        if limit is not None and rows + batch.num_rows > limit:
                            batch = batch.slice(0, limit - rows)
                        if writer is None:
                            writer = Writer(tmp, dst_fmt, dst_comp, schema, o, batch)
                        writer.write(batch)
                        rows += batch.num_rows
                        if progress is not None:
                            done = reader.consumed if reader is not None else 0
                            if progress(done, bytes_in) is False:
                                raise KeyboardInterrupt
                        if limit is not None and rows >= limit:
                            break
                    if writer is None:
                        writer = Writer(tmp, dst_fmt, dst_comp, schema, o, None)
                    writer.close()
                except BaseException:
                    # release the file so the temporary output can be removed (Windows)
                    if writer is not None:
                        writer.abort()
                    raise
            if pool is not None:
                pool.shutdown()
            break
        except Violation as v:
            restarts += 1
            name = reader.names[v.column] if reader else "?"
            if restarts > len(types) + 4:
                raise DataError("could not settle on a schema") from None
            if not full:
                warnings.append(f"column '{name}': value '{v.value}' beyond the sample does not fit "
                                f"{types[v.column].label}; re-planned from a full scan")
                full = True
            elif name in o.schema:
                raise DataError(f"column '{name}': value '{v.value}' cannot be stored as {o.schema[name]}") from None
            else:
                o.schema = dict(o.schema, **{name: "string"})
                warnings.append(f"column '{name}': value '{v.value}' forced the column to text")
        except UnicodeDecodeError:
            restarts += 1
            if dialect is None or dialect.encoding not in ("utf-8", "utf8") or restarts > 3:
                raise DataError("input is not valid in any supported encoding") from None
            warnings.append("input is not valid UTF-8; re-read as windows-1252")
            dialect.encoding = "cp1252"
    bad = reader.bad if reader is not None else []
    if bad and o.bad_lines != "skip":
        qpath = Path(o.quarantine) if o.quarantine else dst.with_name(dst.name + ".rejected.csv")
        with open(qpath, "w", encoding="utf-8", newline="") as qf:
            qf.write(pure.csv_line(reader.names, ",", "\n"))  # type: ignore[union-attr]
            for line in bad:
                qf.write(line + "\n")
        report.quarantine = str(qpath)
        warnings.append(f"{len(bad)} malformed rows moved to {qpath}")
    elif bad:
        warnings.append(f"{len(bad)} malformed rows skipped")
    report.rows = rows
    report.columns = len(schema.names)
    report.schema = ([(n, t.label) for n, t in zip(schema.names, types)] if dialect is not None
                     else [(f.name, str(f.type)) for f in schema])
    report.bad_rows = len(bad)
    report.restarts = restarts
    report.warnings = notes + warnings
    report.dialect = dialect.to_dict() if dialect else None
    report.bytes_out = dst.stat().st_size
    report.seconds = time.perf_counter() - started
    report.compression = dst_fmt if not dst_comp else f"{dst_fmt}/{dst_comp}"
    return report


def info(path: Path, fmt: str, o: ConvertOptions) -> Dict[str, Any]:
    size = path.stat().st_size
    if fmt == "parquet":
        pq = _compat.require("pyarrow.parquet", "reading Parquet", "arrow")
        md = pq.ParquetFile(path).metadata
        cols = []
        for j in range(md.num_columns):
            comp = enc = None
            csize = usize = 0
            nulls: Optional[int] = 0
            for i in range(md.num_row_groups):
                c = md.row_group(i).column(j)
                comp, enc = c.compression, c.encodings
                csize += c.total_compressed_size
                usize += c.total_uncompressed_size
                st = c.statistics
                nulls = None if st is None or nulls is None or not st.has_null_count else nulls + st.null_count
            col = md.schema.column(j)
            cols.append({"name": col.path, "physical_type": col.physical_type, "logical_type": str(col.logical_type),
                         "compression": [comp], "encodings": list(enc or ()), "compressed_bytes": csize,
                         "uncompressed_bytes": usize, "null_count": nulls})
        return {"format": "parquet", "path": str(path), "file_size": size, "rows": md.num_rows,
                "row_groups": md.num_row_groups, "columns": cols, "created_by": md.created_by,
                "format_version": md.format_version}
    schema, _ = read_batches(path, fmt, o)
    return {"format": fmt, "path": str(path), "file_size": size, "rows": None,
            "columns": [{"name": f.name, "type": str(f.type)} for f in schema]}


def head(path: Path, n: int, fmt: str, o: ConvertOptions) -> Dict[str, Any]:
    schema, batches = read_batches(path, fmt, o, batch_rows=max(1, n))
    rows: List[List[Optional[str]]] = []
    for b in batches:
        for r in zip(*(c.to_pylist() for c in b.columns)):
            rows.append([pure.csv_text(v) for v in r])
            if len(rows) >= n:
                break
        if len(rows) >= n:
            break
    return {"columns": list(schema.names), "types": [str(f.type) for f in schema], "rows": rows}


def count(path: Path, fmt: str, o: ConvertOptions) -> int:
    if fmt == "parquet":
        pq = _compat.require("pyarrow.parquet", "reading Parquet", "arrow")
        return int(pq.ParquetFile(path).metadata.num_rows)
    _, batches = read_batches(path, fmt, o)
    return sum(b.num_rows for b in batches)
