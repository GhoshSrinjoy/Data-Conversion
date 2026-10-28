"""Parquet compression profiles and per-column encoding tuning (pyarrow writer).

The native engine has its own tuner; this one serves the arrow engine and
``vegg.write``.  Profiles:

* ``fast``     - zstd level 1, dictionary encoding
* ``balanced`` - zstd level 3, dictionary encoding (widely compatible)
* ``max``      - zstd level 19 and, per column, whichever encoding (dictionary,
  DELTA_BINARY_PACKED, BYTE_STREAM_SPLIT, DELTA_BYTE_ARRAY, ...) produces the
  smallest output on a sample of the data
"""

from __future__ import annotations

import functools
from typing import Any, Dict, List, Optional

from . import _compat
from .options import ConvertOptions

LEVELS = {"fast": 1, "balanced": 3, "max": 19}


def _candidates(pa: Any, ty: Any) -> List[str]:
    t = pa.types
    if t.is_integer(ty) or t.is_date(ty) or t.is_timestamp(ty) or t.is_time(ty):
        return ["DICTIONARY", "DELTA_BINARY_PACKED", "BYTE_STREAM_SPLIT", "PLAIN"]
    if t.is_floating(ty) or t.is_decimal(ty):
        return ["DICTIONARY", "BYTE_STREAM_SPLIT", "PLAIN"]
    if t.is_string(ty) or t.is_large_string(ty) or t.is_binary(ty):
        return ["DICTIONARY", "DELTA_BYTE_ARRAY", "DELTA_LENGTH_BYTE_ARRAY", "PLAIN"]
    return []


@functools.lru_cache(maxsize=None)
def _supported(encoding: str, type_name: str) -> bool:
    """Whether the installed pyarrow can write `encoding` for this type."""
    pa = _compat.require("pyarrow", "Parquet tuning", "arrow")
    pq = _compat.require("pyarrow.parquet", "Parquet tuning", "arrow")
    from decimal import Decimal

    samples = {
        "int64": lambda: pa.array([1, 2, 3], pa.int64()),
        "double": lambda: pa.array([1.5, 2.5]),
        "string": lambda: pa.array(["a", "b"]),
        "decimal": lambda: pa.array([Decimal(1), Decimal(2)], pa.decimal128(10, 0)),
        "date": lambda: pa.array([1, 2], pa.int32()).cast(pa.date32()),
    }
    arr = samples.get(type_name, samples["int64"])()
    try:
        sink = pa.BufferOutputStream()
        pq.write_table(pa.table({"c": arr}), sink, use_dictionary=False, column_encoding={"c": encoding})
        return True
    except Exception:
        return False


def _type_key(pa: Any, ty: Any) -> str:
    t = pa.types
    if t.is_floating(ty):
        return "double"
    if t.is_decimal(ty):
        return "decimal"
    if t.is_string(ty) or t.is_large_string(ty) or t.is_binary(ty):
        return "string"
    if t.is_date(ty):
        return "date"
    return "int64"


def tune_encodings(sample: Any, compression: str, level: Optional[int]) -> Dict[str, str]:
    """Per column, the encoding that writes the sample smallest."""
    pa = _compat.require("pyarrow", "Parquet tuning", "arrow")
    pq = _compat.require("pyarrow.parquet", "Parquet tuning", "arrow")
    table = pa.Table.from_batches([sample]) if isinstance(sample, pa.RecordBatch) else sample
    table = table.slice(0, min(table.num_rows, 200_000))
    chosen: Dict[str, str] = {}
    for field in table.schema:
        cands = _candidates(pa, field.type)
        if not cands or table.num_rows == 0:
            continue
        col = table.select([field.name])
        best = None
        for enc in cands:
            if enc != "DICTIONARY" and not _supported(enc, _type_key(pa, field.type)):
                continue
            sink = pa.BufferOutputStream()
            try:
                kwargs: Dict[str, Any] = {"compression": compression, "compression_level": level}
                if enc == "DICTIONARY":
                    kwargs["use_dictionary"] = True
                else:
                    kwargs.update(use_dictionary=False, column_encoding={field.name: enc})
                pq.write_table(col, sink, **kwargs)
            except Exception:
                continue
            size = sink.getvalue().size
            if best is None or size < best[0]:
                best = (size, enc)
        if best is not None:
            chosen[field.name] = best[1]
    return chosen


def parquet_writer_options(o: ConvertOptions, schema: Any, sample: Any = None) -> Dict[str, Any]:
    """Keyword arguments for ``pyarrow.parquet.ParquetWriter``."""
    compression = (o.compression or "zstd").lower()
    if compression == "auto":
        compression = "zstd"
    if compression == "lz4":
        compression = "lz4"
    level = o.level if o.level is not None else (LEVELS[o.profile] if compression in ("zstd", "gzip", "brotli") else None)
    if compression == "gzip" and level is not None:
        level = min(level, 9)
    if compression == "brotli" and level is not None:
        level = min(level, 11)
    kwargs: Dict[str, Any] = {
        "compression": compression,
        "write_statistics": o.statistics is not False,
    }
    if level is not None and compression not in ("none", "snappy", "lz4"):
        kwargs["compression_level"] = level
    encodings = dict(o.encodings)
    tune = o.tune if o.tune is not None else o.profile == "max"
    if tune and not encodings and sample is not None:
        encodings = tune_encodings(sample, compression, kwargs.get("compression_level"))
    if o.dictionary is False:
        kwargs["use_dictionary"] = False
    plain = {k: v for k, v in encodings.items() if v.upper() not in ("DICTIONARY", "DICT", "RLE_DICTIONARY")}
    if plain:
        dict_cols = [f.name for f in schema if f.name not in plain]
        kwargs["use_dictionary"] = dict_cols if o.dictionary is not False else False
        kwargs["column_encoding"] = {k: v.upper() for k, v in plain.items()}
    if o.parquet_version in ("2.0", "2"):
        kwargs["data_page_version"] = "2.0"
    return kwargs
