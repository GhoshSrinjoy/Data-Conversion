"""Per-column profiles: counts, nulls, distinct values, ranges and the most common values."""

from __future__ import annotations

from typing import Any, Dict, List

from . import _compat
from ._util import PathLike


def stats(path: PathLike, *, top: int = 5, **options: Any) -> List[Dict[str, Any]]:
    """Profile every column of a file (loaded with lossless type inference)."""
    pa = _compat.require("pyarrow", "column statistics", "arrow")
    _compat.require("pyarrow.compute", "column statistics", "arrow")
    pc = pa.compute
    from .api import load

    table = load(path, **options)
    out = []
    for name in table.column_names:
        col = table.column(name)
        entry: Dict[str, Any] = {
            "column": name,
            "type": str(col.type),
            "rows": len(col),
            "nulls": col.null_count,
        }
        try:
            entry["distinct"] = pc.count_distinct(col).as_py()
        except (pa.ArrowNotImplementedError, pa.ArrowInvalid):
            entry["distinct"] = None
        t = col.type
        if pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_decimal(t) or pa.types.is_temporal(t):
            try:
                mm = pc.min_max(col)
                entry["min"], entry["max"] = mm["min"].as_py(), mm["max"].as_py()
            except (pa.ArrowNotImplementedError, pa.ArrowInvalid):
                pass
        if pa.types.is_integer(t) or pa.types.is_floating(t):
            entry["mean"] = pc.mean(col).as_py()
            entry["stddev"] = pc.stddev(col).as_py()
        if pa.types.is_string(t) or pa.types.is_large_string(t):
            lengths = pc.utf8_length(col)
            mm = pc.min_max(lengths)
            entry["min_length"], entry["max_length"] = mm["min"].as_py(), mm["max"].as_py()
        if top and len(col) and (entry.get("distinct") or 0) <= max(1000, len(col) // 2):
            try:
                vc = pc.value_counts(col).to_pylist()
                vc.sort(key=lambda d: -d["counts"])
                entry["top"] = [{"value": d["values"], "count": d["counts"]} for d in vc[:top]]
            except (pa.ArrowNotImplementedError, pa.ArrowInvalid):
                pass
        out.append(entry)
    return out
