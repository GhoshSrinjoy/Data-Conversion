"""Adapter for the Rust engine."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from .. import _native
from ..formats import DELIMITERS
from ..options import ConvertOptions, _native_format
from ..report import Report

Progress = Optional[Callable[[int, int], Any]]


def options(o: ConvertOptions, src_fmt: str, dst_fmt: str) -> Dict[str, Any]:
    """Options for the native engine, including the source format as detected here
    (from magic bytes or content), so both sides read the file the same way."""
    data = o.to_native(dst_fmt, DELIMITERS.get(dst_fmt) if dst_fmt in ("tsv", "psv") else None)
    data["from"] = _native_format(src_fmt)
    if src_fmt in ("tsv", "psv") and o.delimiter is None:
        data.setdefault("sniff", {})["delimiter"] = DELIMITERS[src_fmt]
    return data


def _opts(o: ConvertOptions, src_fmt: str, dst_fmt: str) -> str:
    return _native.options_json(options(o, src_fmt, dst_fmt))


def convert(src: Path, dst: Path, src_fmt: str, dst_fmt: str, o: ConvertOptions, progress: Progress) -> Report:
    raw = _native.call_json("convert", os.fspath(src), os.fspath(dst), _opts(o, src_fmt, dst_fmt), progress)
    return Report.from_dict(raw)


def compress(src: Path, dst: Path, profile: str, threads: Optional[int], block_size: Optional[int], overwrite: bool,
             progress: Progress, memory: Optional[int] = None) -> Report:
    # the memory budget only when set: older native modules take no such keyword
    extra = {"memory": memory} if memory is not None else {}
    raw = _native.call_json("compress", os.fspath(src), os.fspath(dst), profile, threads or 0, block_size, overwrite, progress,
                            **extra)
    return Report.from_dict(raw)


def decompress(src: Path, dst: Path, threads: Optional[int], overwrite: bool, progress: Progress,
               memory: Optional[int] = None) -> Report:
    extra = {"memory": memory} if memory is not None else {}
    raw = _native.call_json("decompress", os.fspath(src), os.fspath(dst), threads or 0, overwrite, progress, **extra)
    return Report.from_dict(raw)


def info(path: Path, exact: bool, o: ConvertOptions, src_fmt: str) -> Dict[str, Any]:
    return _native.call_json("info", os.fspath(path), exact, _opts(o, src_fmt, "ipc"))  # type: ignore[no-any-return]


def head(path: Path, n: int, o: ConvertOptions, src_fmt: str) -> Dict[str, Any]:
    return _native.call_json("head", os.fspath(path), n, _opts(o, src_fmt, "ipc"))  # type: ignore[no-any-return]


def count(path: Path, o: ConvertOptions, src_fmt: str) -> int:
    return int(_native.call("count", os.fspath(path), _opts(o, src_fmt, "ipc")))


def sniff(path: Path, o: ConvertOptions, src_fmt: str) -> Dict[str, Any]:
    return _native.call_json("sniff", os.fspath(path), _opts(o, src_fmt, "ipc"))  # type: ignore[no-any-return]
