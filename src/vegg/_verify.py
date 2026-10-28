"""Prove that two files hold the same data, even across formats."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import _compat, formats
from ._util import PathLike


def _bytes_equal(a: Path, b: Path) -> bool:
    if a.stat().st_size != b.stat().st_size:
        return False
    with open(a, "rb") as fa, open(b, "rb") as fb:
        while True:
            x, y = fa.read(1 << 20), fb.read(1 << 20)
            if x != y:
                return False
            if not x:
                return True


def _same(pa: Any, x: Any, y: Any) -> Any:
    """Element-wise equality where null equals null and NaN equals NaN."""
    pc = pa.compute
    same = pc.or_kleene(pc.equal(x, y), pc.and_(pc.is_null(x), pc.is_null(y)))
    if pa.types.is_floating(x.type) and pa.types.is_floating(y.type):
        same = pc.or_kleene(same, pc.and_(pc.is_nan(x), pc.is_nan(y)))
    return pc.fill_null(same, False)


def _lossless_cast(pa: Any, col: Any, ty: Any) -> Any:
    """``col`` cast to ``ty`` when that keeps every value (casting back gives the same
    column), else None: "007" -> 7 is a cast that would make different data equal."""
    try:
        cast = col.cast(ty)
        back = cast.cast(col.type)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError, OverflowError, ValueError):
        return None
    return cast if pa.compute.all(_same(pa, back, col)).as_py() is not False else None


def _common(pa: Any, ca: Any, cb: Any) -> Tuple[Any, Any]:
    """Both columns in one type, reached without losing anything; text when no type
    holds both sides losslessly (so values that differ never compare equal)."""
    cast_b = _lossless_cast(pa, cb, ca.type)
    if cast_b is not None:
        return ca, cast_b
    cast_a = _lossless_cast(pa, ca, cb.type)
    if cast_a is not None:
        return cast_a, cb
    try:
        return ca.cast(pa.string()), cb.cast(pa.string())
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
        return ca, cb


def digest(path: PathLike) -> str:
    """SHA-256 of a file's bytes."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def verify(a: PathLike, b: PathLike, *, max_differences: int = 10, **options: Any) -> Dict[str, Any]:
    """Compare the contents of two files.

    Identical bytes are reported as such.  Otherwise both files are loaded as
    tables (with the same lossless type inference a conversion uses) and compared
    column by column; values are compared after casting to a common type, so a
    CSV and the Parquet file made from it compare equal.
    """
    pa_, pb = Path(a), Path(b)
    result: Dict[str, Any] = {"a": str(pa_), "b": str(pb), "equal": False, "identical_bytes": False, "differences": []}
    if _bytes_equal(pa_, pb):
        result.update(equal=True, identical_bytes=True)
        return result
    fa, _ = formats.detect(pa_)
    fb, _ = formats.detect(pb)
    if fa == "dcz" or fb == "dcz":
        # DCZ restores bytes exactly: compare the decoded bytes when the other side is text
        from . import dcz as _dcz

        def raw(p: Path, fmt: str) -> Optional[bytes]:
            if fmt == "dcz":
                return b"".join(_dcz.iter_decoded(p))
            if fmt in ("csv", "tsv", "psv"):
                return p.read_bytes()
            return None

        ra, rb = raw(pa_, fa), raw(pb, fb)
        if ra is not None and rb is not None:
            result["equal"] = result["identical_bytes"] = ra == rb
            if ra != rb:
                pos = next((i for i, (x, y) in enumerate(zip(ra, rb)) if x != y), min(len(ra), len(rb)))
                result["differences"].append({"kind": "bytes", "offset": pos})
            return result
    pa = _compat.require("pyarrow", "comparing table contents", "arrow")
    _compat.require("pyarrow.compute", "comparing table contents", "arrow")
    from .api import load

    ta, tb = load(pa_, **options), load(pb, **options)
    diffs: List[Dict[str, Any]] = []
    result["rows"] = [ta.num_rows, tb.num_rows]
    result["columns"] = [ta.column_names, tb.column_names]
    if ta.column_names != tb.column_names:
        diffs.append({"kind": "columns", "a": ta.column_names, "b": tb.column_names})
    if ta.num_rows != tb.num_rows:
        diffs.append({"kind": "rows", "a": ta.num_rows, "b": tb.num_rows})
    pc = pa.compute
    for name in [n for n in ta.column_names if n in tb.column_names]:
        ca, cb = ta.column(name), tb.column(name)
        n = min(len(ca), len(cb))
        ca, cb = ca.slice(0, n), cb.slice(0, n)
        if ca.type != cb.type:
            ca, cb = _common(pa, ca, cb)
        if ca.equals(cb):
            continue
        bad = pc.invert(_same(pa, ca, cb))
        idx = pc.indices_nonzero(bad) if hasattr(pc, "indices_nonzero") else None
        rows = idx.to_pylist()[:max_differences] if idx is not None else []
        if not rows and not pc.any(bad).as_py():
            continue
        for r in rows:
            diffs.append({"kind": "value", "column": name, "row": r, "a": ca[r].as_py(), "b": cb[r].as_py()})
            if len(diffs) >= max_differences:
                break
        if len(diffs) >= max_differences:
            break
    result["differences"] = diffs
    result["equal"] = not diffs
    return result
