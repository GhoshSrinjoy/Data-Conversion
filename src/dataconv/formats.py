"""File formats: names, extensions, magic-byte detection and output naming."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

from .errors import FileError, FormatError


@dataclass(frozen=True)
class Format:
    name: str
    extensions: Tuple[str, ...]
    description: str
    #: Engines able to read / write it.
    native: bool
    arrow: bool
    pure: bool
    writable: bool = True


FORMATS: Dict[str, Format] = {
    f.name: f
    for f in (
        Format("csv", (".csv", ".txt", ".dat"), "comma-separated values", True, True, True),
        Format("tsv", (".tsv", ".tab"), "tab-separated values", True, True, True),
        Format("psv", (".psv",), "pipe-separated values", True, True, True),
        Format("jsonl", (".jsonl", ".ndjson", ".jsonlines", ".ldjson"), "JSON Lines", True, True, True),
        Format("json", (".json",), "JSON array of records", False, True, True),
        Format("parquet", (".parquet", ".pq", ".parq"), "Apache Parquet", True, True, False),
        Format("ipc", (".arrow", ".feather", ".ipc", ".fea"), "Arrow IPC / Feather v2", True, True, False),
        Format("orc", (".orc",), "Apache ORC", False, True, False),
        Format("dcz", (".dcz",), "DCZ lossless columnar compression", True, True, True),
        Format("xlsx", (".xlsx", ".xlsm"), "Excel workbook (read-only)", False, True, False, writable=False),
    )
}

ALIASES = {
    "pq": "parquet",
    "parq": "parquet",
    "ndjson": "jsonl",
    "jsonlines": "jsonl",
    "feather": "ipc",
    "arrow": "ipc",
    "excel": "xlsx",
    "xls": "xlsx",
    "txt": "csv",
}

COMPRESSIONS = {
    ".gz": "gzip",
    ".gzip": "gzip",
    ".bz2": "bz2",
    ".xz": "xz",
    ".lzma": "xz",
    ".zst": "zstd",
    ".zstd": "zstd",
    ".lz4": "lz4",
    ".br": "brotli",
    ".zip": "zip",
}

DELIMITERS = {"csv": ",", "tsv": "\t", "psv": "|"}

_EXT = {ext: f.name for f in FORMATS.values() for ext in f.extensions}

DCZ_MAGIC = b"\x89DCZ\r\n\x1a\n"


def canonical(name: str) -> str:
    key = name.lower().lstrip(".")
    key = ALIASES.get(key, key)
    if key not in FORMATS:
        known = ", ".join(sorted(FORMATS))
        raise FormatError(f"unknown format {name!r} (known: {known})")
    return key


def split_name(path: os.PathLike) -> Tuple[str, Optional[str], Optional[str]]:
    """Split ``data.csv.gz`` into ('data', 'csv', 'gzip')."""
    name = Path(path).name
    stem, ext = os.path.splitext(name)
    compression = COMPRESSIONS.get(ext.lower())
    if compression:
        name = stem
        stem, ext = os.path.splitext(name)
    fmt = _EXT.get(ext.lower())
    if fmt is None:
        stem = name
    return stem, fmt, compression


def _magic(path: Path) -> Tuple[Optional[str], Optional[str]]:
    try:
        with open(path, "rb") as f:
            head = f.read(16)
    except OSError:
        return None, None
    if head.startswith(b"PAR1"):
        return "parquet", None
    if head.startswith(b"ARROW1"):
        return "ipc", None
    if head.startswith(b"\xff\xff\xff\xff") and len(head) >= 8:
        return "ipc", None
    if head.startswith(b"ORC"):
        return "orc", None
    if head.startswith(DCZ_MAGIC):
        return "dcz", None
    if head.startswith(b"\x1f\x8b"):
        return None, "gzip"
    if head.startswith(b"\x28\xb5\x2f\xfd"):
        return None, "zstd"
    if head.startswith(b"\xfd7zXZ\x00"):
        return None, "xz"
    if head.startswith(b"BZh"):
        return None, "bz2"
    if head.startswith(b"\x04\x22\x4d\x18"):
        return None, "lz4"
    if head.startswith(b"PK\x03\x04"):
        return None, "zip"
    return None, None


def detect(path: os.PathLike, explicit: Optional[str] = None) -> Tuple[str, Optional[str]]:
    """(format, compression) of an existing file.  Magic bytes win over extensions."""
    p = Path(path)
    if p.is_dir():
        raise FileError(f"{p} is a directory, not a file")
    magic_fmt, magic_comp = _magic(p)
    _, ext_fmt, ext_comp = split_name(p)
    compression = magic_comp or ext_comp
    if magic_comp == "zip" and ext_fmt == "xlsx":
        compression = None
    if explicit:
        return canonical(explicit), compression
    if magic_fmt:
        return magic_fmt, None
    if ext_fmt:
        return ext_fmt, compression
    if compression is None:
        try:
            with open(p, "rb") as f:
                lead = f.read(4096).lstrip(b"\xef\xbb\xbf \t\r\n")
        except OSError:
            lead = b""
        if lead.startswith(b"["):
            return "json", None
        if lead.startswith(b"{"):
            return "jsonl", None
    return "csv", compression


def target(path: os.PathLike, explicit: Optional[str] = None) -> Tuple[str, Optional[str]]:
    """(format, compression) implied by a destination path."""
    _, fmt, compression = split_name(path)
    if explicit:
        fmt = canonical(explicit)
    if fmt is None:
        raise FormatError(f"cannot tell the output format of {path}; pass to=... (e.g. to='parquet')")
    if not FORMATS[fmt].writable:
        raise FormatError(f"{FORMATS[fmt].description} can only be read")
    return fmt, compression


def default_output(src: os.PathLike, fmt: str, compression: Optional[str] = None) -> Path:
    """``data.csv.gz`` + parquet -> ``data.parquet`` (same directory)."""
    p = Path(src)
    stem, _, _ = split_name(p)
    ext = FORMATS[fmt].extensions[0]
    if fmt == "dcz":
        # keep the original name visible: data.csv -> data.csv.dcz
        _, src_fmt, _ = split_name(p)
        if src_fmt:
            ext = FORMATS[src_fmt].extensions[0] + ".dcz"
    suffix = ""
    if compression:
        suffix = {v: k for k, v in COMPRESSIONS.items() if k not in (".gzip", ".zstd", ".lzma")}[compression]
    return p.with_name(stem + ext + suffix)


def engine_supports(engine: str, src_fmt: str, dst_fmt: str, src_comp: Optional[str], dst_comp: Optional[str]) -> bool:
    src, dst = FORMATS[src_fmt], FORMATS[dst_fmt]
    if engine == "native":
        ok_comp = {None, "gzip", "zstd", "xz", "lz4", "brotli"}
        textual = {"csv", "tsv", "psv", "jsonl"}
        if src_comp not in ok_comp or dst_comp not in ok_comp:
            return False
        if dst_comp and dst_fmt not in textual:
            return False
        return src.native and dst.native
    if engine == "arrow":
        return src.arrow and dst.arrow
    return src.pure and dst.pure
