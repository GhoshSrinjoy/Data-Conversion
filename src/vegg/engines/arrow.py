"""pyarrow engine: Parquet, Arrow IPC/Feather, ORC, CSV, JSON Lines, JSON and Excel.

CSV input is read as text in record-aligned chunks, and every column is cast with
the same lossless guards as the native engine (vectorised with pyarrow.compute).
Records are split by the quote rules every engine shares (see
:mod:`vegg._csvtok`): pyarrow parses the runs of records those rules leave
regular, and the few records with stray, irregular or escaped quotes go through the
Python tokenizer.
"""

from __future__ import annotations

import io
import json
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from pathlib import Path
from typing import IO, Any, Callable, Dict, Iterator, List, Optional, Sequence, Set, Tuple

from .. import _compat, _csvtok, _io, dcz, formats, infer
from .._sniff import Dialect
from .._util import atomic_output, prefetch
from ..errors import DataError, FormatError, SchemaError
from ..options import ConvertOptions
from ..report import Report
from ..resources import make_plan
from . import pure

Progress = Optional[Callable[[int, int], Any]]

_INT_RE = r"^(0|-?[1-9][0-9]*)$"  # -0 is not an integer: only a float keeps its sign
_INT_LOOSE_RE = r"^[+-]?[0-9]+$"
_FLOAT_RE = r"^-?((0|[1-9][0-9]*)(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$|^-?([iI][nN][fF]([iI][nN][iI][tT][yY])?|[nN][aA][nN])$"
_FLOAT_LOOSE_RE = r"^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$|^-?([iI][nN][fF]([iI][nN][iI][tT][yY])?|[nN][aA][nN])$"
_PLAIN_DEC_RE = r"^-?(0|[1-9][0-9]*)?(\.[0-9]*)?$"  # canonical: leading zeros would be lost
_NEG_ZERO_RE = r"^-0*\.?0*$"
_DATE_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
_TS_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]{1,9})?)?(Z|[+-][0-9]{2}(:?[0-9]{2})?)?$"
_BOOLS = ["true", "True", "TRUE", "false", "False", "FALSE"]
_BOM = b"\xef\xbb\xbf"
_ARROW_RUN = 1 << 12  # bytes of regular records worth a pyarrow call of their own (one costs ~0.1 ms)


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


class _Replan(Exception):
    """A value later in a JSON or Excel file does not fit the planned column type."""

    def __init__(self, column: str, value: str) -> None:
        super().__init__(column)
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
        # an empty chunk has nothing to violate (pc.all of nothing is null)
        if len(ok) and not pc.all(pc.fill_null(ok, True)).as_py():
            raise Violation(index, _first_bad(arr, ok))

    try:
        if ty.kind == "bool":
            # is_in is False (not null) for a null: missing values must not fail the check
            require(pc.or_(pc.is_null(arr), pc.is_in(arr, value_set=pa.array(_BOOLS))))
            truthy = pc.is_in(arr, value_set=pa.array(_BOOLS[:3]))
            return pc.if_else(pc.is_null(arr), pa.scalar(None, pa.bool_()), truthy)
        if ty.kind == "int64":
            require(pc.match_substring_regex(arr, _INT_LOOSE_RE if loose else _INT_RE))
            if loose:  # '+5' is 5 in loose mode, as in the other engines
                arr = pc.replace_substring_regex(arr, r"^\+", "")
            return pc.cast(arr, pa.int64())
        if ty.kind == "float64":
            require(pc.match_substring_regex(arr, _FLOAT_LOOSE_RE if loose else _FLOAT_RE))
            if not loose:
                # the exact check of infer.parse_float for every value that could lose
                # something: many digits, or an exponent (overflow to inf, underflow to 0)
                risky = pc.fill_null(pc.or_(pc.greater_equal(pc.utf8_length(arr), 16), pc.match_substring_regex(arr, "[eE]")), False)
                if pc.any(risky).as_py():
                    for v in pc.unique(pc.filter(arr, risky)).to_pylist():
                        info = infer.parse_float(v)
                        if info is None or not info.lossless:
                            raise Violation(index, v)
            return pc.cast(arr, pa.float64())
        if ty.kind == "decimal":
            require(pc.match_substring_regex(arr, _PLAIN_DEC_RE))
            negzero = pc.fill_null(pc.match_substring_regex(arr, _NEG_ZERO_RE), False)
            if pc.any(negzero).as_py():  # a decimal cannot keep the sign of -0
                raise Violation(index, str(pc.filter(arr, negzero)[0].as_py()))
            return pc.cast(arr, pa.decimal128(ty.precision, ty.scale))
        if ty.kind == "date":
            require(pc.match_substring_regex(arr, _DATE_RE))
            return pc.cast(arr, pa.date32())
        if ty.kind == "timestamp":
            require(pc.match_substring_regex(arr, _TS_RE))
            # offsets and precision are checked per distinct value (timestamps repeat little,
            # but this stays exact); the cast itself then runs vectorised
            values = [v for v in pc.unique(arr).to_pylist() if v is not None]
            expect = infer.tz_offset_minutes(ty.tz)
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


class _Utf8(io.RawIOBase):
    """The UTF-8 bytes of a text stream in another encoding: decoding first keeps
    record cuts on character boundaries (a UTF-16 line break is two bytes)."""

    def __init__(self, text: IO[str]) -> None:
        super().__init__()
        self.text = text
        self.pending = b""

    def readable(self) -> bool:
        return True

    def readinto(self, b: Any) -> int:
        while not self.pending:
            t = self.text.read(1 << 18)
            if not t:
                return 0
            self.pending = t.encode("utf-8")
        n = min(len(b), len(self.pending))
        b[:n] = self.pending[:n]
        self.pending = self.pending[n:]
        return n

    def close(self) -> None:
        if not self.closed:
            self.text.close()
        super().close()


class CsvReader:
    """Record-aligned text chunks, every column as strings."""

    def __init__(self, path: Path, fmt: str, dialect: Dialect, o: ConvertOptions, chunk_size: int) -> None:
        self.path, self.fmt, self.dialect, self.o = path, fmt, dialect, o
        self.chunk_size = chunk_size
        self.consumed = 0
        # the record policy (blank lines, malformed rows, padding) of the pure engine
        self.src = pure.CsvSource(path, fmt, dialect, o, text_output=True)
        self.names = list(self.src.names)
        self.utf8 = _io.is_utf8(dialect.encoding)
        self.forced_at: Optional[int] = None  # a quote read as a stray only by its size
        self._cuts: Dict[int, Any] = {}  # chunks already cut, reused by the next pass (see Chunker)

    @property
    def bad(self) -> List[str]:
        return self.src.bad

    def reset(self) -> None:
        self.src.bad = []
        self.src.blank = self.src.irregular = 0

    def _open(self) -> IO[bytes]:
        d = self.dialect
        if self.utf8:
            if self.fmt == "dcz":
                return io.BufferedReader(dcz.DczReader(self.path), 1 << 20)
            return _io.open_binary(self.path)
        return io.BufferedReader(_Utf8(pure._open_csv_text(self.path, self.fmt, d.encoding)), 1 << 20)

    def tables(self) -> Iterator[Any]:
        if self.o.bad_lines == "pad":
            yield from self._padded_tables()
            return
        yield from self._arrow_tables()

    def _table(self, columns: List[List[Optional[str]]]) -> Any:
        pa = _pa()
        return pa.table({n: pa.array(c, pa.string()) for n, c in zip(self.names, columns)})

    def _padded_tables(self) -> Iterator[Any]:
        """Python parsing for bad_lines='pad' (pyarrow can only skip short rows)."""
        batch: List[List[Optional[str]]] = []
        yielded = False
        for fields in self.src.raw_rows():
            batch.append(fields)
            if len(batch) >= 65536:
                yield self._table([list(c) for c in zip(*batch)])
                yielded = True
                batch = []
        if batch or not yielded:  # always at least one (possibly empty) table
            yield self._table([list(c) for c in zip(*batch)] if batch else [[] for _ in self.names])

    def _python_rows(self, data: bytes, tok: _csvtok.Dialect, pos: int = 0, stop: Optional[int] = None) -> Tuple[List[List[Optional[str]]], int]:
        """(rows, end): the records of ``data`` from ``pos``, through the shared
        tokenizer, up to the first one that ends at or after ``stop`` (all of them
        without it)."""
        rows = []
        end = pos
        for values, start, end, _, flags in _csvtok.tokenize(data, tok, pos):
            # invalid UTF-8 restarts the job as windows-1252
            text = data[start:end].decode("utf-8")
            vals = [None if v is None else v.decode("utf-8") for v in values]
            fields = self.src._fields(vals, flags, text, 0, len(text))
            if fields is not None:
                rows.append(fields)
            if stop is not None and end >= stop:
                break
        return rows, end

    def _rejected_text(self, data: bytes, a: int, b: int, texts: List[str], btok: _csvtok.Dialect) -> List[str]:
        """The original text of the records pyarrow rejected in ``data[a:b]`` (``texts``,
        in order, without their line endings), each with its own line ending."""
        run = data[a:b]
        lines = run.splitlines(keepends=True)
        out = []
        if len(lines) == _csvtok.count_records(run, btok):  # every line is a record
            i = 0
            for text in texts:
                want = text.encode("utf-8")
                while i < len(lines) and lines[i].rstrip(b"\r\n") != want:
                    i += 1
                out.append(lines[i].decode("utf-8") if i < len(lines) else text + self.dialect.terminator)
                i += 1
            return out
        # line breaks inside quoted fields: the tokenizer finds the records
        ncols, wide = len(self.dialect.columns), len(self.dialect.columns) != 1
        for values, start, end, _, _ in _csvtok.tokenize(run, btok):
            if len(values) != ncols and not (wide and _csvtok.is_blank(values)):
                out.append(run[start:end].decode("utf-8"))
        return out

    def _rows_table(self, rows: List[List[Optional[str]]]) -> Any:
        return self._table([list(c) for c in zip(*rows)] if rows else [[] for _ in self.names])

    def _arrow_tables(self) -> Iterator[Any]:
        pa = _pa()
        pacsv = pa.csv
        d = self.dialect
        quote = d.quote.encode() if d.quote else None
        convert = pacsv.ConvertOptions(
            column_types={c: pa.string() for c in d.columns},
            strings_can_be_null=True,
            null_values=[""],
            quoted_strings_can_be_null=False,
            include_columns=self.names,
        )
        btok = _csvtok.Dialect.of(d, text=False)
        python_only = d.escape is not None  # pyarrow reads escapes outside quotes too
        single = len(d.columns) == 1

        def read_run(data: bytes, a: int, b: int, records: Optional[int]) -> Any:
            """pyarrow's table of the regular records ``data[a:b]`` (``records`` of them
            when known, else counted when blank lines may have to be)."""
            rejected: List[str] = []
            widths: List[int] = []

            def invalid(row: Any) -> str:
                rejected.append(row.text)
                widths.append(row.actual_columns)
                return "skip"

            parse = pacsv.ParseOptions(
                delimiter=d.delimiter,
                quote_char=d.quote or False,
                double_quote=d.double_quote,
                escape_char=False,
                newlines_in_values=quote is not None and data.find(quote, a, b) >= 0,
                ignore_empty_lines=not single,  # in a one-column file a blank line is a missing value
                invalid_row_handler=invalid,
            )
            read = pacsv.ReadOptions(column_names=d.columns, block_size=max(1 << 20, b - a + 2))
            if b == len(data) and b > a and data[b - 1 :] not in (b"\n", b"\r"):
                # a last record without a line break: pyarrow would read an empty field
                # after a trailing delimiter there as quoted ("") when a quoted one precedes it
                source = pa.py_buffer(data[a:b] + b"\n")
            else:
                source = pa.py_buffer(data)
                if a or b != len(data):
                    source = source.slice(a, b - a)
            try:
                table = pacsv.read_csv(source, read_options=read, parse_options=parse, convert_options=convert)
            except pa.ArrowInvalid as exc:
                if "UTF8" in str(exc) or "utf8" in str(exc).lower():
                    raise UnicodeDecodeError("utf-8", b"", 0, 1, str(exc)) from None
                raise DataError(str(exc)) from None
            if rejected:
                raws = self._rejected_text(data, a, b, rejected, btok)
                if self.o.bad_lines == "error":
                    raise DataError(pure.bad_record_message(raws[0], widths[0], len(d.columns), False))
                self.src.bad.extend(raws)
            if not single and _csvtok.may_hold_blank(data, btok, a, b):  # records pyarrow skipped as empty lines
                if records is None:
                    records = _csvtok.count_records(data[a:b], btok)
                self.src.blank += records - table.num_rows - len(rejected)
            return table

        with self._open() as f:
            head = f.read(3)
            chunker = _csvtok.Chunker(f, self.chunk_size, btok, prefix=head[3:] if head.startswith(_BOM) else head,
                                      known=self._cuts)
            skip = d.leading_records
            for chunk in chunker:
                self.consumed = chunker.offset
                if chunker.forced and self.forced_at is None:
                    self.forced_at = chunker.offset
                pos = 0
                if skip:
                    # the header and any preamble are records too: a quoted line break
                    # inside them must not be taken for the end of a row
                    pos, skipped = _csvtok.skip_records(chunk, btok, skip)
                    skip -= skipped
                    if pos >= len(chunk):
                        continue
                if python_only:
                    yield self._rows_table(self._python_rows(chunk, btok, pos)[0])
                elif not chunker.irregular:
                    yield read_run(chunk, pos, len(chunk), None)
                else:
                    yield self._mixed_table(chunk, btok, pos, read_run)

    def _mixed_table(self, chunk: bytes, btok: _csvtok.Dialect, pos: int, read_run: Callable[..., Any]) -> Any:
        """A chunk with stray or irregular quotes: its runs of regular records through
        pyarrow, the other records through the shared tokenizer (which reads on further
        each time, so that dense irregular records do not each cost a fast-path check)."""
        pa = _pa()
        tables = []
        rows: List[List[Optional[str]]] = []
        n = len(chunk)
        ahead = 0
        while pos < n:
            r, breaks = _csvtok.regular_run(chunk, btok, pos, 1 << 10 if ahead else _csvtok.CUT_WINDOW, count=True)
            if r - pos >= _ARROW_RUN:
                if rows:
                    tables.append(self._rows_table(rows))
                    rows = []
                end_rec = r == n and chunk[-1:] not in (b"\n", b"\r")  # a last record without a terminator
                tables.append(read_run(chunk, pos, r, breaks + (1 if end_rec else 0)))
                pos = r
                ahead = 0
                continue
            more, pos = self._python_rows(chunk, btok, pos, r + 1 + ahead)
            rows += more
            ahead = min(max(ahead * 2, 1 << 10), 1 << 16)
        if rows or not tables:
            tables.append(self._rows_table(rows))
        return tables[0] if len(tables) == 1 else pa.concat_tables(tables)


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
    reader.reset()
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


def _json_batches(path: Path, fmt: str, o: ConvertOptions, batch_rows: int, full: bool,
                  notes: List[str], sample: Optional[int] = None) -> Tuple[Any, Iterator[Any]]:
    """JSON and JSON Lines by the lossless JSON rules every engine shares (see
    :mod:`vegg._jsonplan`), as Arrow: objects as structs, arrays as lists, text
    holding numbers or JSON marked so."""
    pa = _pa()
    src = pure.JsonSource(path, fmt, o, full=full, sample=sample)
    notes.extend(src.warnings)
    schema = pa.schema([p.field(pa, name) for name, p in zip(src.names, src.plans)])

    def batches() -> Iterator[Any]:
        rows = src.rows()
        while True:
            buf = list(islice(rows, batch_rows))
            if not buf:
                return
            if not len(schema):  # records without any key: rows without columns
                yield pa.RecordBatch.from_struct_array(pa.array([{}] * len(buf), pa.struct([])))
            else:
                cols = [pa.array(list(c), schema.field(j).type) for j, c in enumerate(zip(*buf))]
                yield pa.RecordBatch.from_arrays(cols, schema=schema)
            if len(buf) < batch_rows:
                return

    return schema, batches()


def _xlsx_batches(path: Path, o: ConvertOptions, batch_rows: int, full: bool, text_columns: Set[str]) -> Tuple[Any, Iterator[Any]]:
    """Excel rows; a column mixing kinds of values (text and numbers) is text."""
    pa = _pa()
    names, rows = _xlsx_rows(path)
    cols = o.columns
    if cols:
        missing = [c for c in cols if c not in names]
        if missing:
            raise FormatError(f"no column named {missing[0]!r}")
        idx = [names.index(c) for c in cols]
        names = list(cols)
        rows = ([r[i] for i in idx] for r in rows)
    first: List[List[Any]] = []
    for r in rows:
        first.append(r)
        if not full and len(first) >= batch_rows:
            break
    fields = []
    for j, n in enumerate(names):
        values = [r[j] for r in first]
        t = None
        if n not in text_columns:
            try:
                t = pa.array(values).type if values else pa.string()
            except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError, OverflowError, TypeError, ValueError):
                t = None
            if t is not None and pa.types.is_null(t):
                t = pa.string()
        fields.append(pa.field(n, t if t is not None else pa.string()))
        if t is None:
            text_columns.add(n)
    schema = pa.schema(fields)

    def batch(buf: List[List[Any]]) -> Any:
        columns = list(zip(*buf)) if buf else [() for _ in names]
        arrays = []
        for c, f in zip(columns, schema):
            if f.name in text_columns:
                arrays.append(pa.array([pure.csv_text(v) for v in c], pa.string()))
                continue
            try:
                arrays.append(pa.array(list(c), f.type))
            except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, TypeError, ValueError):
                bad = next((v for v in c if v is not None), None)
                raise _Replan(f.name, str(bad)[:80]) from None
        return pa.RecordBatch.from_arrays(arrays, schema=schema)

    def more() -> Iterator[Any]:
        for k in range(0, len(first), batch_rows):
            yield batch(first[k : k + batch_rows])
        if not first:
            yield batch([])
        buf: List[List[Any]] = []
        for r in rows:
            buf.append(r)
            if len(buf) >= batch_rows:
                yield batch(buf)
                buf = []
        if buf:
            yield batch(buf)

    return schema, more()


def read_feather_v1(path: Path, columns: Optional[List[str]] = None) -> Any:
    """A Feather version 1 file (from before the Arrow IPC format), which only pyarrow's
    feather module reads (and calls deprecated: it is still read here)."""
    import warnings

    feather = _compat.require("pyarrow.feather", "reading Feather version 1", "arrow")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        try:
            return feather.read_table(str(path), columns=columns or None)
        except (_pa().ArrowInvalid, _pa().ArrowNotImplementedError, OSError) as exc:
            raise FormatError(f"{path} is a Feather version 1 file this pyarrow cannot read: {exc}") from None


def _valid_batches(path: Path, batches: Iterator[Any]) -> Iterator[Any]:
    """Arrow file batches, each checked in full before use: offsets or text that a
    corrupt file gets wrong would otherwise crash the process when read."""
    pa = _pa()
    while True:
        try:
            batch = next(batches)
            for col in batch.columns:
                # a date64 may hold any milliseconds, which a full check refuses; its
                # fixed-width values cannot misdirect a read anyway
                col.validate(full=not pa.types.is_date64(col.type))
        except StopIteration:
            return
        except (pa.ArrowException, OSError) as exc:
            raise DataError(f"{path.name} is corrupt: {exc}") from None
        yield batch


def read_batches(path: Path, fmt: str, o: ConvertOptions, batch_rows: int = 65536, full: bool = False,
                 text_columns: Optional[Set[str]] = None, notes: Optional[List[str]] = None,
                 sample: Optional[int] = None) -> Tuple[Any, Iterator[Any]]:
    """(schema, iterator of RecordBatches) for typed formats."""
    pa = _pa()
    cols = o.columns
    text_columns = set() if text_columns is None else text_columns
    notes = [] if notes is None else notes
    if fmt == "parquet":
        pq = _compat.require("pyarrow.parquet", "reading Parquet", "arrow")
        pf = pq.ParquetFile(path)
        schema = pf.schema_arrow if not cols else pa.schema([pf.schema_arrow.field(c) for c in cols])
        return schema, pf.iter_batches(batch_size=batch_rows, columns=cols, use_threads=True)
    if fmt == "ipc" and formats.is_feather_v1(path):
        table = read_feather_v1(path, cols)
        return table.schema, iter(table.to_batches(max_chunksize=batch_rows))
    if fmt == "ipc":
        source = pa.memory_map(str(path), "r")
        try:
            try:
                reader = pa.ipc.open_file(source)
                raw: Iterator[Any] = (reader.get_batch(i) for i in range(reader.num_record_batches))
            except pa.ArrowInvalid:
                reader = pa.ipc.open_stream(source)
                raw = iter(reader)
        except (pa.ArrowException, OSError) as exc:  # pyarrow reports a damaged stream as OSError
            raise DataError(f"{path.name} is not a readable Arrow file: {exc}") from None
        schema = reader.schema
        batches = _valid_batches(path, raw)
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
    if fmt in ("json", "jsonl"):
        return _json_batches(path, fmt, o, batch_rows, full, notes, sample)
    if fmt == "xlsx":
        return _xlsx_batches(path, o, batch_rows, full, text_columns)
    raise FormatError(f"the arrow engine cannot read {fmt}")


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


def _ticks(pa: Any, arr: Any) -> List[Optional[int]]:
    return arr.cast(pa.int64()).to_pylist()  # type: ignore[no-any-return]


def _is(pa: Any, check: str, t: Any) -> bool:
    """``pa.types.<check>(t)``, false where this pyarrow lacks the type (view types)."""
    f = getattr(pa.types, check, None)
    return f is not None and bool(f(t))


def _rows(arr: Any) -> List[bool]:
    """Whether each row of ``arr`` holds a value."""
    return [True] * len(arr) if arr.null_count == 0 else arr.is_valid().to_pylist()  # type: ignore[no-any-return]


def _key_text(k: Any) -> str:
    """A map key as a JSON object key: its JSON, made a string when it is not one (``1``
    becomes ``"1"``), as the native writers do."""
    text = pure.json_text(k)
    return text if text[:1] == '"' else pure._encode_str(text)  # type: ignore[no-any-return]


def _nested_texts(pa: Any, arr: Any, t: Any) -> List[Any]:
    """The JSON text of each struct, list or map (None for a null), built from the values
    of its children, which are formatted like columns of their own.  Maps are objects,
    entries in order, repeated keys included."""
    text = pure.JsonText
    of = pure.json_text
    if pa.types.is_struct(t):
        fields = [t.field(i) for i in range(t.num_fields)]
        children = [py_values(arr.field(i), f) for i, f in enumerate(fields)]
        names = pure.json_keys([f.name for f in fields])
        return [
            text("{" + ",".join(k + of(c[r]) for k, c in zip(names, children)) + "}") if ok else None
            for r, ok in enumerate(_rows(arr))
        ]
    if len(arr) == 0:
        return []
    # offsets follow the slice of ``arr``; values, keys and items are the whole child
    offsets = arr.offsets.to_pylist()
    start, end = offsets[0], offsets[-1]
    bounds = [(offsets[r] - start, offsets[r + 1] - start) for r in range(len(arr))]
    if pa.types.is_map(t):
        keys = [_key_text(k) for k in py_values(arr.keys.slice(start, end - start), t.key_field)]
        items = py_values(arr.items.slice(start, end - start), t.item_field)
        return [
            text("{" + ",".join(keys[i] + ":" + of(items[i]) for i in range(a, b)) + "}") if ok else None
            for (a, b), ok in zip(bounds, _rows(arr))
        ]
    items = py_values(arr.values.slice(start, end - start), t.value_field)
    return [text("[" + ",".join(of(x) for x in items[a:b]) + "]") if ok else None for (a, b), ok in zip(bounds, _rows(arr))]


def type_name(t: Any) -> str:
    """The name of an Arrow type in reports, ``head``, ``info`` and ``schema``, the same
    from every engine: the type rules' names where a type has one (``int64``,
    ``timestamp[ms,+05:30]``, ``decimal(12,2)``), pyarrow's otherwise (``uint8``,
    ``large_string``, ``list<int64>``, ``struct<a: int64>``)."""
    pa = _pa()
    types = pa.types
    if types.is_boolean(t):
        return "bool"
    if types.is_floating(t):
        return f"float{t.bit_width}"
    if types.is_string(t):
        return "string"
    if types.is_date32(t):
        return "date"
    if types.is_date64(t):
        return "date64"
    if types.is_timestamp(t):
        return f"timestamp[{t.unit}]" if t.tz is None else f"timestamp[{t.unit},{t.tz}]"
    if types.is_decimal(t):
        return f"decimal({t.precision},{t.scale})"
    if types.is_fixed_size_list(t):
        return f"fixed_size_list<{type_name(t.value_type)}>[{t.list_size}]"
    for check, name in (("is_list", "list"), ("is_large_list", "large_list"), ("is_list_view", "list_view"),
                        ("is_large_list_view", "large_list_view")):
        if _is(pa, check, t):
            return f"{name}<{type_name(t.value_type)}>"
    if types.is_struct(t):
        return "struct<" + ", ".join(f"{t.field(i).name}: {type_name(t.field(i).type)}" for i in range(t.num_fields)) + ">"
    if types.is_map(t):
        return f"map<{type_name(t.key_type)}, {type_name(t.item_type)}>"
    if types.is_dictionary(t):
        return f"dictionary<values={type_name(t.value_type)}, indices={type_name(t.index_type)}>"
    if _is(pa, "is_run_end_encoded", t):
        return f"run_end_encoded<run_ends={type_name(t.run_end_type)}, values={type_name(t.value_type)}>"
    if types.is_union(t):
        return "union"
    return str(t)  # integers, binary, times, durations, intervals and null: pyarrow's names


def _list_view_texts(arr: Any, t: Any) -> List[Any]:
    """List views as JSON text: list ``r`` is ``sizes[r]`` values from ``offsets[r]``
    (pyarrow's cast to a list drops null items)."""
    if len(arr) == 0:
        return []
    items = py_values(arr.values, t.value_field)
    of = pure.json_text
    return [
        pure.JsonText("[" + ",".join(of(x) for x in items[o : o + s]) + "]") if ok else None
        for o, s, ok in zip(arr.offsets.to_pylist(), arr.sizes.to_pylist(), _rows(arr))
    ]


_SINGLE_SPECIAL = {"nan": float("nan"), "inf": float("inf"), "-inf": float("-inf")}


def py_values(arr: Any, field: Any = None) -> List[Any]:
    """Python values of an Arrow column for the text writers, formatted exactly like the
    native writers: temporal values from their ticks (any year, down to nanoseconds,
    without a time zone database), float32 with its own shortest digits, binary values
    as hex, text marked as JSON (``field``'s metadata) as the JSON it holds, and
    structs, lists and maps as their JSON text."""
    pa = _pa()
    t = arr.type
    if pa.types.is_dictionary(t):
        arr = arr.cast(t.value_type)
        t = arr.type
    if pa.types.is_string(t) or pa.types.is_large_string(t) or _is(pa, "is_string_view", t):
        values = arr.to_pylist()
        kind = (field.metadata or {}).get(pure.JSON_TEXT_KEY.encode()) if field is not None else None
        if kind == b"number":  # the NaN / Infinity texts stay strings: JSON has no such numbers
            return [v if v is None or v in pure._SPECIAL_FLOATS else pure.JsonText(v) for v in values]
        if kind == b"value":
            return [v if v is None else pure.JsonText(v) for v in values]
        return values  # type: ignore[no-any-return]
    if pa.types.is_fixed_size_list(t):
        arr = arr.cast(pa.list_(t.value_field))
        t = arr.type
    elif _is(pa, "is_list_view", t) or _is(pa, "is_large_list_view", t):
        return _list_view_texts(arr, t)
    if pa.types.is_struct(t) or pa.types.is_map(t) or pa.types.is_list(t) or pa.types.is_large_list(t):
        return _nested_texts(pa, arr, t)
    if pa.types.is_timestamp(t):
        offset = infer.tz_offset_minutes(t.tz)
        return [None if v is None else infer.format_timestamp(v, t.unit, offset) for v in _ticks(pa, arr)]
    if pa.types.is_date32(t):
        return [None if v is None else infer.format_date(v) for v in arr.cast(pa.int32()).to_pylist()]
    if pa.types.is_date64(t):  # a value that is not a whole day keeps its time
        return [
            None if v is None else infer.format_date(v // 86_400_000) if v % 86_400_000 == 0 else infer.format_timestamp(v, "ms", None)
            for v in _ticks(pa, arr)
        ]
    if pa.types.is_time32(t) or pa.types.is_time64(t):
        ticks = arr.cast(pa.int32() if pa.types.is_time32(t) else pa.int64()).to_pylist()
        return [None if v is None else infer.format_time(v, t.unit) for v in ticks]
    if pa.types.is_duration(t):
        return [None if v is None else infer.format_duration(v, t.unit) for v in _ticks(pa, arr)]
    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t) or _is(pa, "is_binary_view", t):
        return [None if v is None else v.hex() for v in arr.to_pylist()]
    try:
        if pa.types.is_float16(t):  # written as the float32 it widens to
            arr = arr.cast(pa.float32())
            t = arr.type
        if pa.types.is_float32(t):
            # pyarrow writes each value with its own shortest digits; JsonText keeps the
            # text as it is in CSV and JSON alike
            return [
                None if s is None else _SINGLE_SPECIAL[s] if s in _SINGLE_SPECIAL else pure.JsonText(pure.single_repr(s))
                for s in arr.cast(pa.string()).to_pylist()
            ]
        return arr.to_pylist()  # type: ignore[no-any-return]
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, ValueError) as exc:
        raise DataError(f"cannot write a {t} column as text: {exc}") from None


def _write_error(fmt: str, exc: Exception) -> Exception:
    """A pyarrow error while writing, as one of ours: a type the format cannot hold is a
    schema error, anything else a data error."""
    what = {"parquet": "Parquet", "ipc": "Arrow", "orc": "ORC"}.get(fmt, fmt)
    kind = SchemaError if isinstance(exc, _pa().ArrowNotImplementedError) else DataError
    return kind(f"cannot write {what}: {exc}")


def _holds(pa: Any, t: Any, check: Callable[[Any], bool]) -> bool:
    """Whether type ``t`` is, or nests, a type that ``check`` accepts."""
    if check(t):
        return True
    if pa.types.is_struct(t):
        return any(_holds(pa, t.field(i).type, check) for i in range(t.num_fields))
    if pa.types.is_map(t):
        return _holds(pa, t.key_type, check) or _holds(pa, t.item_type, check)
    if pa.types.is_dictionary(t):
        return _holds(pa, t.value_type, check)
    value_type = getattr(t, "value_type", None)
    return value_type is not None and _holds(pa, value_type, check)


def _check_date64(pa: Any, name: str, arr: Any) -> None:
    """A Parquet date cannot keep a time of day: refuse a date64 value that has one, as
    the native engine does (pyarrow would drop it)."""
    t = arr.type
    if pa.types.is_date64(t):
        ticks = arr.cast(pa.int64())
        days = pa.compute.multiply(pa.compute.divide(ticks, 86_400_000), 86_400_000)
        timed = pa.compute.filter(ticks, pa.compute.not_equal(ticks, days))
        if len(timed):
            raise SchemaError(f"column '{name}': date64 value {timed[0].as_py()} has a time of day, which a "
                              "Parquet date cannot keep; cast the column to a timestamp first")
    elif pa.types.is_struct(t):
        for i in range(t.num_fields):
            _check_date64(pa, name, arr.field(i))
    elif pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
        _check_date64(pa, name, arr.flatten())
    elif pa.types.is_map(t):
        _check_date64(pa, name, arr.keys)
        _check_date64(pa, name, arr.items)


def _without_dictionary_nulls(pa: Any, batch: Any) -> Any:
    """A dictionary holding a null among its values, with that null moved to the
    indices: pyarrow cannot write the former to Parquet."""
    cols = list(batch.columns)
    moved = False
    for i, col in enumerate(cols):
        t = col.type
        if pa.types.is_dictionary(t) and col.dictionary.null_count:
            cols[i] = col.cast(t.value_type).dictionary_encode().cast(t)
            moved = True
    return pa.RecordBatch.from_arrays(cols, schema=batch.schema) if moved else batch


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

            options = parquet_writer_options(o, schema, sample)
            # an explicit sink, closed when the writer cannot be made, so the temporary
            # file can be removed (Windows)
            self._stream = pa.OSFile(str(path), "wb")
            try:
                self._inner = pq.ParquetWriter(self._stream, schema, **options)
            except pa.ArrowException as exc:
                self._stream.close()
                raise _write_error("Parquet", exc) from None
            # as the native engine writes: the max profile shares dictionaries over more rows
            self.row_group = o.row_group_rows or (1 << 20 if o.profile == "max" else 1 << 18)
            self._date64 = any(_holds(pa, f.type, pa.types.is_date64) for f in schema)
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
            schema = batch.schema
            cols = [py_values(col, schema.field(j)) for j, col in enumerate(batch.columns)]
            for row in zip(*cols) if cols else [()] * batch.num_rows:
                self._text.write(row)
        else:
            pa = _pa()
            try:
                if self.fmt == "parquet":
                    if self._date64:
                        for f, col in zip(batch.schema, batch.columns):
                            _check_date64(pa, f.name, col)
                    self._inner.write_batch(_without_dictionary_nulls(pa, batch), row_group_size=self.row_group)
                elif self.fmt == "orc":
                    self._inner.write(pa.Table.from_batches([batch]))
                else:
                    self._inner.write_batch(batch)
            except pa.ArrowException as exc:
                raise _write_error(self.fmt, exc) from None

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
        if table.num_rows == 0:
            continue
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
        return pure.compress(src, dst, o.profile, o.overwrite, progress, transparent=True)
    warnings: List[str] = []
    restarts = 0
    is_csv = src_fmt in pure.DELIMITED
    quarantine = pure.Quarantine(src, dst, o, is_csv)
    dialect = _dialect(src, src_fmt, o) if is_csv else None
    text_out = dst_fmt in ("csv", "tsv", "psv", "dcz")
    mode = "string" if (text_out and not o.schema) else o.types
    full = o.infer == "full"
    types: List[infer.LogicalType] = []
    text_columns: Set[str] = set()
    chunk = max(4 << 20, min(plan.chunk_size * 8, 128 << 20))
    first_notes: Optional[List[str]] = None  # CSV: the notes of the first plan, as the native engine reports them
    while True:
        reader = None
        notes: List[str] = []
        try:
            if dialect is not None:
                reader = CsvReader(src, src_fmt, dialect, o, chunk)
                types, notes = _infer(reader, o, mode, full)
                if first_notes is None:
                    first_notes = notes
                names = reader.names
                schema = pa.schema([pa.field(n, t.arrow()) for n, t in zip(names, types)])
                pool = ThreadPoolExecutor(plan.threads)
                stream = _cast_batches(reader, names, types, schema, mode, o.null_values, pool)
            else:
                schema, stream = read_batches(src, src_fmt, o, full=full, text_columns=text_columns, notes=notes)
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
        except (_Replan, pure._Restart) as r:
            # a JSON or Excel value (or key) beyond the sample: plan again from everything
            restarts += 1
            if restarts > len(text_columns) + 8:
                raise DataError("could not settle on a schema") from None
            if isinstance(r, pure._Restart) and r.reason == "json":
                if full:
                    raise DataError(r.value) from None
                warnings.append(f"{r.value}; re-planned from a full scan")
                full = True
                continue
            if isinstance(r, _Replan):
                what = f"column '{r.column}': value '{r.value}'"
            elif r.reason == "columns":
                what = f"field '{r.value}'"
            else:
                what = f"value '{r.value}'"
            if not full:
                warnings.append(f"{what} beyond the sample does not fit the planned schema; re-planned from a full scan")
                full = True
            elif isinstance(r, _Replan) and r.column not in text_columns:
                text_columns.add(r.column)
                warnings.append(f"{what} forced the column to text")
            else:
                raise DataError(f"{what} does not fit any type") from None
        except UnicodeDecodeError:
            restarts += 1
            if dialect is None or _io.native_label(dialect.encoding) != "utf-8" or restarts > 3:
                raise DataError("input is not valid in any supported encoding") from None
            warnings.append("input is not valid UTF-8; re-read as windows-1252")
            dialect.encoding = "cp1252"
    bad = reader.bad if reader is not None else []
    found: List[str] = []  # what the conversion itself found
    if reader is not None:
        forced = reader.forced_at if reader.forced_at is not None else reader.src.forced_at
        if forced is not None:
            found.append(_csvtok.forced_warning(forced))
    if dialect is not None:
        quarantine.finish(dialect.columns, dialect.delimiter, bad, report, found)  # whole records: every column
    report.rows = rows
    report.columns = len(schema.names)
    report.schema = ([(n, t.label) for n, t in zip(schema.names, types)] if dialect is not None
                     else [(f.name, type_name(f.type)) for f in schema])
    report.bad_rows = len(bad)
    report.blank_lines = reader.src.blank if reader is not None else 0
    report.irregular_records = reader.src.irregular if reader is not None else 0
    report.restarts = restarts
    # in the native engine's order: CSV, the conversion's findings, then the first plan's
    # notes and the restarts; other input, the restarts, then the final plan's notes
    report.warnings = found + (first_notes or []) + warnings if dialect is not None else warnings + notes
    report.dialect = dialect.to_dict() if dialect else None
    report.bytes_out = dst.stat().st_size
    report.seconds = time.perf_counter() - started
    report.compression = dst_fmt if not dst_comp else f"{dst_fmt}/{dst_comp}"
    return report


_PQ_UNITS = {"milliseconds": "ms", "microseconds": "us", "nanoseconds": "ns"}


def _logical_name(lt: Any) -> Optional[str]:
    """A Parquet logical type as every engine names it: ``string``, ``int8``, ``uint32``,
    ``decimal(10,2)``, ``date``, ``time[us]``, ``timestamp[ms,utc]``, ``unknown``,
    ``json``; None for none."""
    d = json.loads(lt.to_json())
    kind = d.get("Type", "None")
    if kind == "None":
        return None
    if kind == "Int":
        return ("" if d["isSigned"] else "u") + f"int{d['bitWidth']}"
    if kind == "Decimal":
        return f"decimal({d['precision']},{d['scale']})"
    if kind in ("Time", "Timestamp"):
        return f"{kind.lower()}[{_PQ_UNITS.get(d['timeUnit'], d['timeUnit'])}{',utc' if d['isAdjustedToUTC'] else ''}]"
    return "unknown" if kind == "Null" else str(kind).lower()


def info(path: Path, fmt: str, o: ConvertOptions) -> Dict[str, Any]:
    size = path.stat().st_size
    if fmt == "parquet":
        pq = _compat.require("pyarrow.parquet", "reading Parquet", "arrow")
        md = pq.ParquetFile(path).metadata
        cols = []
        for j in range(md.num_columns):
            codecs: Set[str] = set()
            encodings: Set[str] = set()
            csize = usize = 0
            nulls: Optional[int] = 0
            for i in range(md.num_row_groups):
                c = md.row_group(i).column(j)
                # codec names as the native engine gives them (files do not store levels)
                codecs.add({"UNCOMPRESSED": "none", "LZ4_RAW": "lz4"}.get(c.compression, c.compression.lower()))
                encodings.update(c.encodings)
                csize += c.total_compressed_size
                usize += c.total_uncompressed_size
                st = c.statistics
                nulls = None if st is None or nulls is None or not st.has_null_count else nulls + st.null_count
            col = md.schema.column(j)
            cols.append({"name": col.path, "physical_type": col.physical_type, "logical_type": _logical_name(col.logical_type),
                         "compression": sorted(codecs), "encodings": sorted(encodings), "compressed_bytes": csize,
                         "uncompressed_bytes": usize, "null_count": nulls})
        keys = [k.decode("utf-8", "replace") for k in md.metadata] if md.metadata else None
        return {"format": "parquet", "path": str(path), "file_size": size, "rows": md.num_rows,
                "row_groups": md.num_row_groups, "columns": cols, "created_by": md.created_by,
                "format_version": int(str(md.format_version).split(".")[0]),
                "uncompressed_bytes": sum(md.row_group(i).total_byte_size for i in range(md.num_row_groups)),
                "metadata_keys": keys}
    schema, _ = read_batches(path, fmt, o)
    return {"format": fmt, "path": str(path), "file_size": size, "rows": None,
            "columns": [{"name": f.name, "type": type_name(f.type)} for f in schema]}


def head(path: Path, n: int, fmt: str, o: ConvertOptions) -> Dict[str, Any]:
    # JSON: the types of the first n records, as the native engine's head gives them
    schema, batches = read_batches(path, fmt, o, batch_rows=max(1, n), sample=max(0, n))
    rows: List[List[Optional[str]]] = []
    try:
        for b in batches if n > 0 else ():  # a record beyond a JSON sample of none would re-plan
            cols = [py_values(c) for c in b.columns]
            for r in zip(*cols) if cols else [()] * b.num_rows:
                rows.append([pure.csv_text(v) for v in r])
                if len(rows) >= n:
                    break
            if len(rows) >= n:
                break
    except pure._Restart as r:
        # planned from these very records: a value that does not fit has no type at all
        raise DataError(r.value or "a value fits no type") from None
    return {"columns": list(schema.names), "types": [type_name(f.type) for f in schema], "rows": rows}


def count(path: Path, fmt: str, o: ConvertOptions) -> int:
    if fmt == "parquet":
        pq = _compat.require("pyarrow.parquet", "reading Parquet", "arrow")
        return int(pq.ParquetFile(path).metadata.num_rows)
    if fmt in ("json", "jsonl"):
        return pure.count(path, fmt, o)
    _, batches = read_batches(path, fmt, o)
    return sum(b.num_rows for b in batches)
