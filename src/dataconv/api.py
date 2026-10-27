"""The public API.  Every function picks the best engine available on this machine."""

from __future__ import annotations

import array
import fnmatch
import json
import os
import struct
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from . import _compat, _native, codec, dcz, engines, formats, resources
from ._util import PathLike, atomic_output, parse_size
from .engines import arrow as arrow_engine
from .engines import native as native_engine
from .engines import pure as pure_engine
from .errors import FormatError, MissingDependencyError, OptionsError
from .options import ConvertOptions
from .progress import Progress
from .report import BatchReport, Report

ProgressArg = Union[None, bool, Callable[[int, int], Any]]


def _progress(arg: ProgressArg, label: str) -> Tuple[Optional[Callable[[int, int], Any]], Optional[Progress]]:
    if arg is True:
        bar = Progress(label)
        return bar, bar
    if callable(arg):
        return arg, None
    return None, None


def _engine_for(engine: str, src_fmt: str, dst_fmt: str, src_comp: Optional[str], dst_comp: Optional[str]) -> str:
    return engines.choose(engine, src_fmt, dst_fmt, src_comp, dst_comp)


# ---------------------------------------------------------------------------------
# Conversion
# ---------------------------------------------------------------------------------


def convert(src: PathLike, dst: Optional[PathLike] = None, *, progress: ProgressArg = None, **options: Any) -> Union[Report, BatchReport]:
    """Convert between CSV/TSV, JSON Lines, JSON, Parquet, Arrow IPC/Feather, ORC, DCZ and Excel.

    ``dst`` may be a file, a directory, or omitted when ``to=`` names the format.
    A directory ``src`` converts every supported file under it (see ``convert_dir``).

    Common options: ``to``, ``profile`` (fast/balanced/max), ``compression``,
    ``level``, ``types`` (infer/string/loose), ``schema`` ({"col": "type"} or
    "col=type,..."), ``delimiter``, ``encoding``, ``header``, ``bad_lines``
    (quarantine/skip/error/pad), ``columns``, ``limit``, ``threads``, ``memory``,
    ``overwrite``, ``engine`` (auto/native/arrow/pure).
    """
    src = Path(src)
    if src.is_dir():
        return convert_dir(src, dst, progress=progress, **options)
    workers_opts = {k: options.pop(k) for k in ("include", "exclude", "recursive", "skip_existing", "workers") if k in options}
    if workers_opts:
        raise OptionsError(f"{', '.join(workers_opts)} only apply when converting a directory")
    o = ConvertOptions.build(**options)
    if not src.exists():
        raise FileNotFoundError(f"no such file: {src}")
    src_fmt, src_comp = formats.detect(src, o.source_format)
    if dst is None:
        if o.to is None:
            raise OptionsError("give a destination path or to='format'")
        dst_path = formats.default_output(src, formats.canonical(o.to))
    else:
        dst_path = Path(dst)
        if dst_path.is_dir() or str(dst).endswith(("/", os.sep)):
            if o.to is None:
                raise OptionsError("converting into a directory needs to='format'")
            dst_path = dst_path / formats.default_output(src, formats.canonical(o.to)).name
    dst_fmt, dst_comp = formats.target(dst_path, o.to)
    if dst_path.exists() and dst_path.resolve() == src.resolve():
        raise OptionsError("source and destination are the same file")
    engine = _engine_for(o.engine, src_fmt, dst_fmt, src_comp, dst_comp)
    callback, bar = _progress(progress, f"{src.name} -> {dst_path.name}")
    try:
        if engine == "native":
            return native_engine.convert(src, dst_path, src_fmt, dst_fmt, o, callback)
        if engine == "arrow":
            return arrow_engine.convert(src, dst_path, src_fmt, dst_fmt, dst_comp, o, callback)
        return pure_engine.convert(src, dst_path, src_fmt, dst_fmt, dst_comp, o, callback)
    finally:
        if bar is not None:
            bar.close()


def _matches(path: Path, include: Sequence[str], exclude: Sequence[str]) -> bool:
    name = path.name
    if include and not any(fnmatch.fnmatch(name, p) for p in include):
        return False
    return not any(fnmatch.fnmatch(name, p) for p in exclude)


def convert_dir(
    src: PathLike,
    dst: Optional[PathLike] = None,
    *,
    to: Optional[str] = None,
    include: Union[str, Sequence[str], None] = None,
    exclude: Union[str, Sequence[str], None] = None,
    recursive: bool = True,
    skip_existing: bool = False,
    workers: Optional[int] = None,
    progress: ProgressArg = None,
    **options: Any,
) -> BatchReport:
    """Convert every supported file under ``src`` into ``dst`` (mirroring the tree).

    Files are converted in parallel; the number of simultaneous jobs and the threads
    each one gets adapt to the file sizes and the machine.
    """
    started = time.perf_counter()
    if to is None:
        raise OptionsError("converting a directory needs to='format'")
    fmt = formats.canonical(to)
    root = Path(src)
    out_root = Path(dst) if dst is not None else root
    inc = [include] if isinstance(include, str) else list(include or [])
    exc = [exclude] if isinstance(exclude, str) else list(exclude or [])
    walker = root.rglob("*") if recursive else root.glob("*")
    files = []
    for p in sorted(walker):
        if not p.is_file() or p.name.startswith(".") or not _matches(p, inc, exc):
            continue
        if out_root.resolve() in p.resolve().parents and out_root.resolve() != root.resolve():
            continue
        try:
            src_fmt, _ = formats.detect(p)
        except FormatError:
            continue
        _, ext_fmt, _ = formats.split_name(p)
        if ext_fmt is None and src_fmt == "csv":
            continue  # unknown extension sniffed as text: not a data file
        if p.suffix.lower() in (".txt", ".dat") and not inc:
            continue  # ambiguous text files only when asked for with include="*.txt"
        if src_fmt == fmt and out_root == root:
            continue
        files.append(p)
    total = sum(p.stat().st_size for p in files)
    plan = resources.make_plan(total, options.get("threads"), options.get("memory"), files=max(1, len(files)))
    nworkers = workers or plan.workers
    threads_each = max(1, plan.threads // max(1, nworkers))
    batch = BatchReport()
    jobs = []
    for p in files:
        rel = p.relative_to(root)
        target = (out_root / rel).parent / formats.default_output(p, fmt).name
        if skip_existing and target.exists() and target.stat().st_mtime >= p.stat().st_mtime:
            batch.skipped.append(str(p))
            continue
        jobs.append((p, target))
    done_bytes = [0]
    callback, bar = _progress(progress, f"{len(jobs)} files")

    def run(job: Tuple[Path, Path]) -> Report:
        p, target = job
        target.parent.mkdir(parents=True, exist_ok=True)
        opts = dict(options, to=fmt)
        opts.setdefault("threads", threads_each)
        return convert(p, target, **opts)  # type: ignore[return-value]

    try:
        with ThreadPoolExecutor(max_workers=max(1, nworkers)) as pool:
            futures = {pool.submit(run, job): job for job in jobs}
            for fut in as_completed(futures):
                p, _ = futures[fut]
                try:
                    batch.reports.append(fut.result())
                except Exception as exc_:  # keep going: one bad file must not stop the batch
                    batch.failed.append((str(p), f"{type(exc_).__name__}: {exc_}"))
                done_bytes[0] += p.stat().st_size
                if callback is not None:
                    callback(done_bytes[0], total)
    finally:
        if bar is not None:
            bar.close()
    batch.reports.sort(key=lambda r: r.source)
    batch.seconds = time.perf_counter() - started
    return batch


def split(src: PathLike, out_dir: PathLike, *, rows: Optional[int] = None, size: Union[None, int, str] = None,
          to: Optional[str] = None, pattern: str = "part-{n:05}", extension: Optional[str] = None,
          progress: ProgressArg = None, **options: Any) -> Report:
    """Split a file into parts of at most ``rows`` rows or ``size`` bytes ('100MB').

    CSV to CSV is byte-exact (records are copied verbatim, every part keeps the
    header).  Other formats stream through the converter; for size targets the
    rows per part adapt to the measured output size so parts land near the target.
    """
    target_bytes = parse_size(size, "m") if size is not None else None
    if rows is None and target_bytes is None:
        raise OptionsError("split needs rows=... or size=...")
    src = Path(src)
    src_fmt, src_comp = formats.detect(src, options.get("from_") or options.get("source_format"))
    fmt = formats.canonical(to) if to else src_fmt
    o = ConvertOptions.build(to=fmt, **options)
    ext = extension or formats.FORMATS[fmt].extensions[0]
    engine = engines.choose(o.engine, src_fmt, fmt, src_comp, None)
    callback, bar = _progress(progress, f"split {src.name}")
    try:
        if engine == "native":
            data = o.to_native(fmt, formats.DELIMITERS.get(fmt) if fmt in ("tsv", "psv") else None)
            data.update(split_rows=rows, split_bytes=target_bytes, split_pattern=pattern, split_extension=ext)
            if src_fmt in ("tsv", "psv") and o.delimiter is None:
                data.setdefault("sniff", {})["delimiter"] = formats.DELIMITERS[src_fmt]
            raw = _native.call_json("convert", os.fspath(src), os.fspath(out_dir), _native.options_json(data), callback)
            return Report.from_dict(raw)
        return _split_python(src, Path(out_dir), src_fmt, fmt, rows, target_bytes, pattern, ext, o, callback)
    finally:
        if bar is not None:
            bar.close()


def _split_python(src: Path, out_dir: Path, src_fmt: str, fmt: str, rows: Optional[int], target: Optional[int],
                  pattern: str, ext: str, o: ConvertOptions, callback: Any) -> Report:
    """Fallback splitter: write parts through a temporary full conversion."""
    started = time.perf_counter()
    out_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        staged = Path(tmp) / f"all.{ 'arrow' if _compat.have('pyarrow') else 'jsonl'}"
        convert(src, staged, **{**o.to_dict(), "to": None, "overwrite": True, "limit": o.limit, "columns": o.columns})
        reader = load(staged) if _compat.have("pyarrow") else None
        report = Report(source=str(src), destination=str(out_dir), source_format=src_fmt, destination_format=fmt,
                        engine="arrow" if reader is not None else "pure")
        if reader is None:
            raise MissingDependencyError('splitting without the native engine needs pyarrow: pip install "dataconv[arrow]"')
        n = reader.num_rows
        per = rows or n
        if target:
            sample = reader.slice(0, min(n, 65536))
            probe = Path(tmp) / f"probe{ext}"
            write(sample, probe, **{**o.to_dict(), "to": fmt, "overwrite": True})
            bpr = max(1.0, probe.stat().st_size / max(1, sample.num_rows))
            per = max(1, min(per, int(target / bpr)))
        start = 0
        k = 0
        while start < n:
            part = reader.slice(start, per)
            path = out_dir / f"{part_name(pattern, k)}{ext}"
            write(part, path, **{**o.to_dict(), "to": fmt})
            size = path.stat().st_size
            report.parts.append({"path": str(path), "rows": part.num_rows, "bytes": size})
            report.bytes_out += size
            start += per
            k += 1
            if target and size:
                per = max(1, int(target / (size / max(1, part.num_rows))))
        report.rows = n
        report.bytes_in = src.stat().st_size
        report.seconds = time.perf_counter() - started
    return report


def merge(sources: Sequence[PathLike], dst: PathLike, *, progress: ProgressArg = None, **options: Any) -> Report:
    """Concatenate several files into one, unifying their schemas: missing columns
    become nulls, numeric types widen, and columns whose types conflict become text."""
    pa = _compat.require("pyarrow", "merging files", "arrow")
    started = time.perf_counter()
    srcs = [Path(s) for s in sources]
    if not srcs:
        raise OptionsError("merge needs at least one source")
    o = ConvertOptions.build(**options)
    dst_path = Path(dst)
    dst_fmt, _ = formats.target(dst_path, o.to)
    read_opts = {k: v for k, v in options.items() if k in ("types", "schema", "delimiter", "encoding", "header", "null_values", "columns")}
    tables = [load(s, **read_opts) for s in srcs]

    def concat(ts: List[Any]) -> Any:
        if _compat.pyarrow_version() >= (14, 0, 0):
            return pa.concat_tables(ts, promote_options="permissive")
        return pa.concat_tables(ts, promote=True)

    try:
        final = concat(tables)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
        # Conflicting types: keep every value by storing those columns as text.
        types: Dict[str, set] = {}
        for t in tables:
            for f in t.schema:
                types.setdefault(f.name, set()).add(str(f.type))
        conflicted = {n for n, ts in types.items() if len(ts) > 1}
        fixed = []
        for t in tables:
            cols = [t.column(n).cast(pa.string()) if n in conflicted else t.column(n) for n in t.column_names]
            fixed.append(pa.table(cols, names=t.column_names))
        final = concat(fixed)
    write(final, dst_path, **{k: v for k, v in o.to_dict().items() if k in ("profile", "compression", "level", "overwrite", "to", "engine")})
    return Report(source=", ".join(map(str, srcs)), destination=str(dst_path), source_format="mixed",
                  destination_format=dst_fmt, engine="native" if _native.available() else "arrow",
                  rows=final.num_rows, columns=final.num_columns,
                  bytes_in=sum(x.stat().st_size for x in srcs), bytes_out=dst_path.stat().st_size,
                  seconds=time.perf_counter() - started, schema=[(f.name, str(f.type)) for f in final.schema])


def part_name(pattern: str, n: int) -> str:
    """'part-{n:05}' -> 'part-00007' (the same rule the native engine uses)."""
    import re as _re

    m = _re.search(r"\{n(?::(0?)(\d+))?\}", pattern)
    if not m:
        return f"{pattern}{n:05d}"
    width = int(m.group(2) or 0)
    number = str(n).zfill(width) if m.group(1) else str(n)
    return pattern[: m.start()] + number + pattern[m.end():]


# ---------------------------------------------------------------------------------
# Loading and writing in memory
# ---------------------------------------------------------------------------------


def load(path: PathLike, **options: Any) -> Any:
    """Read a whole file into a ``pyarrow.Table`` with lossless type inference."""
    pa = _compat.require("pyarrow", "load()", "arrow")
    path = Path(path)
    fmt, _ = formats.detect(path, options.get("from_") or options.get("source_format"))
    if fmt == "ipc":
        with pa.OSFile(str(path), "rb") as f:
            return pa.ipc.open_file(f).read_all()
    if fmt == "parquet" and not options:
        import pyarrow.parquet as pq

        return pq.read_table(path)
    with tempfile.TemporaryDirectory() as tmp:
        staged = Path(tmp) / "data.arrow"
        convert(path, staged, **dict(options, to="ipc", compression="none", overwrite=True))
        # read into memory (not a memory map) so the temporary file can be deleted
        with pa.OSFile(str(staged), "rb") as f:
            return pa.ipc.open_file(f).read_all()


def write(data: Any, dst: PathLike, *, progress: ProgressArg = None, **options: Any) -> Report:
    """Write a pyarrow Table/RecordBatch, pandas or polars DataFrame, or a list of
    dicts to any supported format (Parquet uses the selected profile and tuning)."""
    pa = _compat.require("pyarrow", "write()", "arrow")
    table = to_arrow(data)
    dst = Path(dst)
    o = ConvertOptions.build(**options)
    dst_fmt, dst_comp = formats.target(dst, o.to)
    started = time.perf_counter()
    if _native.available() and o.engine in ("auto", "native") and formats.FORMATS[dst_fmt].native:
        with tempfile.TemporaryDirectory() as tmp:
            staged = Path(tmp) / "data.arrow"
            # an explicit sink: a writer opened from a path keeps the file open on Windows
            with pa.OSFile(str(staged), "wb") as sink, pa.ipc.new_file(sink, table.schema) as w:
                w.write_table(table)
            return convert(staged, dst, progress=progress, **options)  # type: ignore[return-value]
    with atomic_output(dst, overwrite=o.overwrite) as tmp:
        writer = arrow_engine.Writer(tmp, dst_fmt, dst_comp, table.schema, o, table.slice(0, 200_000))
        for b in table.to_batches(max_chunksize=1 << 20):
            writer.write(b)
        writer.close()
    return Report(source="<memory>", destination=str(dst), source_format="arrow", destination_format=dst_fmt,
                  engine="arrow", rows=table.num_rows, columns=table.num_columns, bytes_in=table.nbytes,
                  bytes_out=dst.stat().st_size, seconds=time.perf_counter() - started,
                  schema=[(f.name, str(f.type)) for f in table.schema])


def to_arrow(data: Any) -> Any:
    pa = _compat.require("pyarrow", "converting in-memory data", "arrow")
    if isinstance(data, pa.Table):
        return data
    if isinstance(data, pa.RecordBatch):
        return pa.Table.from_batches([data])
    mod = type(data).__module__.split(".")[0]
    if mod == "pandas":
        return pa.Table.from_pandas(data, preserve_index=False)
    if mod == "polars":
        return data.to_arrow()
    if hasattr(data, "__arrow_c_stream__"):
        return pa.table(data)
    if isinstance(data, list):
        return pa.Table.from_pylist(data)
    if isinstance(data, dict):
        return pa.table(data)
    raise TypeError(f"cannot convert {type(data).__name__} to a table")


# ---------------------------------------------------------------------------------
# Compression
# ---------------------------------------------------------------------------------


def compress(src: PathLike, dst: Optional[PathLike] = None, *, profile: str = "balanced", threads: Optional[int] = None,
             block_size: Union[None, int, str] = None, overwrite: bool = False, engine: str = "auto",
             progress: ProgressArg = None) -> Report:
    """Losslessly compress any file into DCZ.

    Delimited text (CSV, TSV, logs) is encoded column by column - integers,
    decimals, timestamps and IDs become compact numeric streams - and decompresses
    to the identical bytes.  Other files are stored as compressed raw blocks.
    """
    if profile not in ("fast", "balanced", "max"):
        raise OptionsError("profile must be fast, balanced or max")
    src = Path(src)
    dst_path = Path(dst) if dst is not None else src.with_name(src.name + ".dcz")
    size = parse_size(block_size)
    callback, bar = _progress(progress, f"compress {src.name}")
    try:
        if engine in ("auto", "native") and _native.available():
            _, comp = formats.detect(src)
            if comp in (None, "gzip", "zstd", "xz", "lz4", "brotli"):
                return native_engine.compress(src, dst_path, profile, threads, size, overwrite, callback)
        if engine == "native":
            raise MissingDependencyError('the native engine is not installed (pip install "dataconv[native]")')
        return pure_engine.compress(src, dst_path, profile, overwrite, callback, size)
    finally:
        if bar is not None:
            bar.close()


def decompress(src: PathLike, dst: Optional[PathLike] = None, *, threads: Optional[int] = None, overwrite: bool = False,
               engine: str = "auto", progress: ProgressArg = None) -> Report:
    """Restore the original bytes of a DCZ file (every checksum is verified)."""
    src = Path(src)
    if dst is None:
        dst_path = src.with_suffix("") if src.suffix.lower() == ".dcz" else src.with_name(src.name + ".out")
    else:
        dst_path = Path(dst)
    callback, bar = _progress(progress, f"decompress {src.name}")
    try:
        if engine in ("auto", "native") and _native.available():
            return native_engine.decompress(src, dst_path, threads, overwrite, callback)
        if engine == "native":
            raise MissingDependencyError('the native engine is not installed (pip install "dataconv[native]")')
        return pure_engine.decompress(src, dst_path, overwrite, callback)
    finally:
        if bar is not None:
            bar.close()


# ---------------------------------------------------------------------------------
# Packing values
# ---------------------------------------------------------------------------------

_I64 = (-(1 << 63), (1 << 63) - 1)


def _classify(values: Sequence[Any]) -> str:
    kind = None
    for v in values:
        if v is None:
            continue
        t = "bool" if isinstance(v, bool) else "int" if isinstance(v, int) else "float" if isinstance(v, float) else "str" if isinstance(v, str) else "json"
        if kind is None:
            kind = t
        elif kind != t:
            return "json"
    return kind or "str"


def pack(values: Any, *, profile: str = "balanced") -> bytes:
    """Losslessly compress a list, tuple, array.array or numpy array.

    Integers use delta / frame-of-reference / run-length coding, floats are kept
    bit-exact (decimal-looking floats become integer mantissas), strings use
    dictionary or numeric-template coding.  ``unpack`` restores the exact values,
    container type, numpy dtype and shape.
    """
    if profile not in ("fast", "balanced", "max"):
        raise OptionsError("profile must be fast, balanced or max")
    np = _compat.optional("numpy")
    tag: Dict[str, Any] = {"v": 1}
    if np is not None and isinstance(values, np.ndarray):
        arr = values
        tag.update(container="ndarray", dtype=arr.dtype.str, shape=list(arr.shape))
        flat = arr.reshape(-1)
        if arr.dtype.kind in "iu" and arr.dtype.itemsize <= 8 and not (arr.dtype.kind == "u" and arr.dtype.itemsize == 8 and flat.size and int(flat.max()) > _I64[1]):
            return _pack("int", flat.astype("<i8").tobytes(), None, tag, profile)
        if arr.dtype.kind == "f" and arr.dtype.itemsize <= 8:
            return _pack("float", flat.astype("<f8").tobytes(), None, tag, profile)
        if arr.dtype.kind == "b":
            return _pack("bool", flat.astype("u1").tobytes(), None, tag, profile)
        if arr.dtype.kind in "Mm":  # datetime64 / timedelta64: exact as int64 ticks
            return _pack("int", np.ascontiguousarray(flat).view("<i8").tobytes(), None, tag, profile)
        values = flat.tolist()
        tag["container"] = "ndarray-list"
    elif isinstance(values, array.array):
        tag.update(container="array", typecode=values.typecode)
        values = values.tolist()
    else:
        tag["container"] = "tuple" if isinstance(values, tuple) else "list"
        values = list(values)
    kind = _classify(values)
    valid = None if all(v is not None for v in values) else bytes(v is not None for v in values)
    present = [v for v in values if v is not None]
    if kind == "int":
        try:
            buf = array.array("q", present).tobytes()
            return _pack("int", buf, valid, tag, profile)
        except OverflowError:
            tag["bigint"] = True
            return _pack("str", [str(v) for v in present], valid, tag, profile)
    if kind == "float":
        return _pack("float", struct.pack(f"<{len(present)}d", *present), valid, tag, profile)
    if kind == "bool":
        return _pack("bool", bytes(present), valid, tag, profile)
    if kind == "str":
        return _pack("str", present, valid, tag, profile)
    tag["json"] = True
    text = json.dumps(values, ensure_ascii=False, separators=(",", ":"), allow_nan=True)
    return _pack("str", [text], None, tag, profile)


def _pack(kind: str, values: Any, valid: Optional[bytes], tag: Dict[str, Any], profile: str) -> bytes:
    tag_bytes = json.dumps(tag, separators=(",", ":")).encode()
    if _native.available():
        return _native.call("pack_values", kind, values, valid, tag_bytes, profile)  # type: ignore[no-any-return]
    kind_id = {"int": 1, "float": 2, "str": 3, "bool": 4}[kind]
    if kind == "int":
        present: Sequence[Any] = list(struct.unpack(f"<{len(values) // 8}q", values))
    elif kind == "float":
        present = list(struct.unpack(f"<{len(values) // 8}d", values))
    elif kind == "bool":
        present = [bool(b) for b in values]
    else:
        present = [s.encode("utf-8") for s in values]
    mask = [bool(b) for b in valid] if valid is not None else None
    return dcz.wrap_packed(dcz.pack_payload(kind_id, present, mask, tag_bytes, profile))


def unpack(data: bytes) -> Any:
    """Inverse of ``pack``."""
    if _native.available():
        kind, raw, valid, tag_bytes = _native.call("unpack_values", bytes(data))
        tag = json.loads(bytes(tag_bytes) or b"{}")
        if tag.get("container") == "ndarray" and valid is None and kind in ("int", "float", "bool"):
            np = _compat.require("numpy", "unpacking numpy arrays", "fast")
            dtype = np.dtype(tag["dtype"])
            base = np.frombuffer(raw, dtype={"int": "<i8", "float": "<f8", "bool": "?"}[kind])
            out = base.view(dtype) if dtype.kind in "Mm" else base.astype(dtype)
            return out.reshape(tag["shape"])
        if kind == "int":
            values: List[Any] = list(struct.unpack(f"<{len(raw) // 8}q", raw))
        elif kind == "float":
            values = list(struct.unpack(f"<{len(raw) // 8}d", raw))
        elif kind == "bool":
            values = [bool(b) for b in raw]
        else:
            values = list(raw)
        mask = None if valid is None else [bool(b) for b in valid]
    else:
        kid, values, mask, tag_bytes = dcz.unpack_payload(dcz.unwrap_packed(bytes(data)))
        kind = {1: "int", 2: "float", 3: "str", 4: "bool"}[kid]
        if kind == "str":
            values = [v.decode("utf-8") for v in values]
    tag = json.loads(bytes(tag_bytes) or b"{}")
    if tag.get("json"):
        return json.loads(values[0])
    if tag.get("bigint"):
        values = [int(v) for v in values]
    if mask is not None:
        it = iter(values)
        values = [next(it) if m else None for m in mask]
    container = tag.get("container", "list")
    if container == "ndarray":
        np = _compat.require("numpy", "unpacking numpy arrays", "fast")
        dtype = np.dtype(tag["dtype"])
        base = np.array(values, dtype={"int": "<i8", "float": "<f8", "bool": "?"}[kind])
        out = base.view(dtype) if dtype.kind in "Mm" else base.astype(dtype)
        return out.reshape(tag["shape"])
    if container == "ndarray-list":
        np = _compat.require("numpy", "unpacking numpy arrays", "fast")
        return np.array(values, dtype=np.dtype(tag["dtype"])).reshape(tag["shape"])
    if container == "array":
        return array.array(tag["typecode"], values)
    if container == "tuple":
        return tuple(values)
    return values


# ---------------------------------------------------------------------------------
# Inspection
# ---------------------------------------------------------------------------------


def _inspect_engine(path: Path, o: ConvertOptions) -> Tuple[str, str]:
    fmt, comp = formats.detect(path, o.source_format)
    for candidate in engines.installed():
        if o.engine not in ("auto", candidate):
            continue
        if formats.engine_supports(candidate, fmt, "ipc" if candidate != "pure" else "csv", comp, None):
            return candidate, fmt
    raise MissingDependencyError(f"reading {fmt} needs pyarrow or the native engine")


def info(path: PathLike, *, exact: bool = False, **options: Any) -> Dict[str, Any]:
    """Describe a file: format, size, rows, schema and format-specific details
    (Parquet row groups, codecs and statistics come from metadata alone)."""
    p = Path(path)
    o = ConvertOptions.build(**options)
    engine, fmt = _inspect_engine(p, o)
    if engine == "native":
        data = native_engine.info(p, exact, o, fmt)
    elif engine == "arrow" and fmt not in ("csv", "tsv", "psv", "dcz"):
        data = arrow_engine.info(p, fmt, o)
        if exact and data.get("rows") is None:
            data["rows"] = arrow_engine.count(p, fmt, o)
    else:
        preview = pure_engine.head(p, 1000, fmt, o)
        data = {"format": fmt, "path": str(p), "file_size": p.stat().st_size,
                "rows": pure_engine.count(p, fmt, o) if exact else None,
                "columns": [{"name": n, "type": t} for n, t in zip(preview["columns"], preview["types"])]}
    if fmt == "dcz" and "original_size" not in data:
        footer = dcz.read_footer(p)
        data.update({k: v for k, v in footer.items() if k != "blocks"})
    data["engine"] = engine
    return data


def head(path: PathLike, n: int = 10, **options: Any) -> Dict[str, Any]:
    """The first ``n`` rows as text: {"columns": [...], "types": [...], "rows": [[...], ...]}."""
    p = Path(path)
    o = ConvertOptions.build(**options)
    engine, fmt = _inspect_engine(p, o)
    if engine == "native":
        return native_engine.head(p, n, o, fmt)
    if engine == "arrow" and fmt not in ("csv", "tsv", "psv", "dcz"):
        return arrow_engine.head(p, n, fmt, o)
    return pure_engine.head(p, n, fmt, o)


def count(path: PathLike, **options: Any) -> int:
    """Exact number of data rows (Parquet: from metadata; CSV: parallel quote-aware scan)."""
    p = Path(path)
    o = ConvertOptions.build(**options)
    engine, fmt = _inspect_engine(p, o)
    if engine == "native":
        return native_engine.count(p, o, fmt)
    if engine == "arrow" and fmt not in ("csv", "tsv", "psv", "dcz"):
        return arrow_engine.count(p, fmt, o)
    return pure_engine.count(p, fmt, o)


def sniff(path: PathLike, **options: Any) -> Dict[str, Any]:
    """Detected CSV dialect: encoding, delimiter, quoting, header, columns."""
    p = Path(path)
    o = ConvertOptions.build(**options)
    fmt, comp = formats.detect(p, o.source_format)
    if _native.available() and o.engine in ("auto", "native") and comp in (None, "gzip", "zstd", "xz", "lz4", "brotli"):
        return native_engine.sniff(p, o, fmt)
    return pure_engine._dialect(p, fmt, o).to_dict()


def schema(path: PathLike, **options: Any) -> List[Tuple[str, str]]:
    """Column names and the types a conversion would use."""
    h = head(path, 1000, **options)
    return list(zip(h["columns"], h["types"]))


# ---------------------------------------------------------------------------------
# Machine
# ---------------------------------------------------------------------------------


def system_info() -> Dict[str, Any]:
    """What this machine offers: OS, CPUs, memory (container-aware), engines, codecs."""
    info_ = resources.detect().to_dict()
    info_["engines"] = engines.installed()
    info_["native_version"] = _native.version()
    info_["codecs"] = codec.available_names()
    info_["packages"] = _compat.features()
    if _native.available():
        info_["native_view"] = _native.call_json("system_info")
    return info_


def plan(input_size: Union[None, int, str] = None, *, threads: Optional[int] = None, memory: Union[None, int, str] = None) -> Dict[str, Any]:
    """How a job of this size would use the machine (threads, memory budget, chunking)."""
    size = parse_size(input_size, "m") if isinstance(input_size, str) else input_size
    if _native.available():
        return _native.call_json("plan", size, threads, parse_size(memory, "m"), None)  # type: ignore[no-any-return]
    return resources.make_plan(size, threads, memory).to_dict()
