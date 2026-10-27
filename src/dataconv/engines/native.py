"""Adapter for the Rust engine."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from .. import _native
from ..formats import DELIMITERS
from ..options import ConvertOptions
from ..report import Report

Progress = Optional[Callable[[int, int], Any]]


def _opts(o: ConvertOptions, src_fmt: str, dst_fmt: str) -> str:
    data = o.to_native(dst_fmt, DELIMITERS.get(dst_fmt) if dst_fmt in ("tsv", "psv") else None)
    if src_fmt in ("tsv", "psv") and o.delimiter is None:
        data.setdefault("sniff", {})["delimiter"] = DELIMITERS[src_fmt]
    return _native.options_json(data)


def convert(src: Path, dst: Path, src_fmt: str, dst_fmt: str, o: ConvertOptions, progress: Progress) -> Report:
    raw = _native.call_json("convert", os.fspath(src), os.fspath(dst), _opts(o, src_fmt, dst_fmt), progress)
    return Report.from_dict(raw)


def compress(src: Path, dst: Path, profile: str, threads: Optional[int], block_size: Optional[int], overwrite: bool, progress: Progress) -> Report:
    raw = _native.call_json("compress", os.fspath(src), os.fspath(dst), profile, threads or 0, block_size, overwrite, progress)
    return Report.from_dict(raw)


def decompress(src: Path, dst: Path, threads: Optional[int], overwrite: bool, progress: Progress) -> Report:
    raw = _native.call_json("decompress", os.fspath(src), os.fspath(dst), threads or 0, overwrite, progress)
    return Report.from_dict(raw)


def info(path: Path, exact: bool, o: ConvertOptions, src_fmt: str) -> Dict[str, Any]:
    return _native.call_json("info", os.fspath(path), exact, _opts(o, src_fmt, "ipc"))  # type: ignore[no-any-return]


def head(path: Path, n: int, o: ConvertOptions, src_fmt: str) -> Dict[str, Any]:
    return _native.call_json("head", os.fspath(path), n, _opts(o, src_fmt, "ipc"))  # type: ignore[no-any-return]


def count(path: Path, o: ConvertOptions, src_fmt: str) -> int:
    return int(_native.call("count", os.fspath(path), _opts(o, src_fmt, "ipc")))


def sniff(path: Path, o: ConvertOptions, src_fmt: str) -> Dict[str, Any]:
    return _native.call_json("sniff", os.fspath(path), _opts(o, src_fmt, "ipc"))  # type: ignore[no-any-return]
