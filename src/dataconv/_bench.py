"""Benchmark formats and codecs on your own data, and recommend one."""

from __future__ import annotations

import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import _native, engines
from ._util import PathLike, format_duration, format_rate, format_size

# (label, destination file name, convert options)
FULL = [
    ("parquet snappy", "a.parquet", {"compression": "snappy"}),
    ("parquet lz4", "b.parquet", {"compression": "lz4"}),
    ("parquet zstd-1", "c.parquet", {"compression": "zstd", "level": 1}),
    ("parquet zstd-3", "d.parquet", {"compression": "zstd", "level": 3}),
    ("parquet zstd-9", "e.parquet", {"compression": "zstd", "level": 9}),
    ("parquet max (tuned)", "f.parquet", {"profile": "max"}),
    ("parquet gzip", "g.parquet", {"compression": "gzip"}),
    ("parquet brotli", "h.parquet", {"compression": "brotli", "level": 9}),
    ("arrow lz4", "i.arrow", {"compression": "lz4"}),
    ("arrow zstd", "j.arrow", {"compression": "zstd"}),
    ("csv.gz", "k.csv.gz", {}),
    ("csv.zst", "l.csv.zst", {"level": 9}),
    ("csv.xz", "m.csv.xz", {"level": 6}),
    ("dcz fast", "n.dcz", {"profile": "fast"}),
    ("dcz balanced", "o.dcz", {"profile": "balanced"}),
    ("dcz max", "p.dcz", {"profile": "max"}),
]
QUICK = ["parquet snappy", "parquet zstd-3", "parquet max (tuned)", "arrow lz4", "csv.zst", "dcz balanced", "dcz max"]


def bench(src: PathLike, *, quick: bool = False, read: bool = True, keep: Optional[PathLike] = None, **options: Any) -> Dict[str, Any]:
    """Convert ``src`` with every format/codec combination and measure it.

    Returns {"source", "bytes_in", "results": [...], "recommendations": {...}}.
    Each result has the output size, compression ratio, write and read times.
    """
    from .api import compress, convert, decompress

    src = Path(src)
    size_in = src.stat().st_size
    cases = [c for c in FULL if not quick or c[0] in QUICK]
    have_dcz_text = True
    tmp = Path(keep) if keep else Path(tempfile.mkdtemp(prefix="dataconv-bench-"))
    tmp.mkdir(parents=True, exist_ok=True)
    results: List[Dict[str, Any]] = []
    try:
        for label, name, opts in cases:
            dst = tmp / name
            entry: Dict[str, Any] = {"case": label, "path": str(dst)}
            try:
                t0 = time.perf_counter()
                if name.endswith(".dcz") and have_dcz_text:
                    compress(src, dst, profile=opts["profile"], overwrite=True)
                else:
                    convert(src, dst, overwrite=True, **{**options, **opts})
                entry["write_seconds"] = time.perf_counter() - t0
                entry["bytes"] = dst.stat().st_size
                entry["ratio"] = size_in / entry["bytes"] if entry["bytes"] else 0.0
                if read:
                    back = tmp / f"read-{name}.arrow"
                    t0 = time.perf_counter()
                    if name.endswith(".dcz"):
                        decompress(dst, tmp / f"read-{name}.raw", overwrite=True)
                    else:
                        convert(dst, back, overwrite=True, to="ipc", compression="none")
                    entry["read_seconds"] = time.perf_counter() - t0
            except Exception as exc:  # report and keep benchmarking the rest
                entry["error"] = f"{type(exc).__name__}: {exc}"
            results.append(entry)
    finally:
        if not keep:
            shutil.rmtree(tmp, ignore_errors=True)
    ok = [r for r in results if "error" not in r]
    rec: Dict[str, str] = {}
    if ok:
        rec["smallest"] = min(ok, key=lambda r: r["bytes"])["case"]
        rec["fastest_write"] = min(ok, key=lambda r: r["write_seconds"])["case"]
        if read:
            rec["fastest_read"] = min(ok, key=lambda r: r.get("read_seconds", 1e9))["case"]
        # balance: size weighted by the fourth root of the total time
        rec["balanced"] = min(ok, key=lambda r: r["bytes"] * (r["write_seconds"] + r.get("read_seconds", 0.0) + 1e-3) ** 0.25)["case"]
        analytics = [r for r in ok if r["case"].startswith("parquet")]
        if analytics:
            rec["for_analytics"] = min(analytics, key=lambda r: r["bytes"] * (r["write_seconds"] + 1e-3) ** 0.25)["case"]
        archive = [r for r in ok if r["case"].startswith("dcz") or r["case"].startswith("csv.")]
        if archive:
            rec["for_archival"] = min(archive, key=lambda r: r["bytes"])["case"]
    return {
        "source": str(src),
        "bytes_in": size_in,
        "engines": engines.installed(),
        "native": _native.version(),
        "results": results,
        "recommendations": rec,
    }


def format_table(report: Dict[str, Any]) -> str:
    rows = [("case", "size", "ratio", "write", "read")]
    for r in sorted(report["results"], key=lambda x: x.get("bytes", 1 << 62)):
        if "error" in r:
            rows.append((r["case"], "-", "-", "-", r["error"][:40]))
            continue
        read = f"{format_duration(r['read_seconds'])}" if "read_seconds" in r else "-"
        rows.append((
            r["case"],
            format_size(r["bytes"]),
            f"{r['ratio']:.1f}x",
            f"{format_duration(r['write_seconds'])} ({format_rate(report['bytes_in'], r['write_seconds'])})",
            read,
        ))
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
    lines = ["  ".join(cell.ljust(w) for cell, w in zip(row, widths)).rstrip() for row in rows]
    lines.insert(1, "  ".join("-" * w for w in widths))
    rec = report["recommendations"]
    if rec:
        lines.append("")
        labels = {
            "smallest": "smallest file",
            "fastest_write": "fastest to write",
            "fastest_read": "fastest to read",
            "balanced": "best balance",
            "for_analytics": "for analytics (Parquet)",
            "for_archival": "for lossless archival",
        }
        for key, label in labels.items():
            if key in rec:
                lines.append(f"{label + ':':26s}{rec[key]}")
    return "\n".join(lines)
