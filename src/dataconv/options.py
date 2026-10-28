"""Validated conversion options shared by every engine."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields
from typing import Any, Dict, List, Optional, Union

from ._io import NATIVE_ENCODINGS as _NATIVE_ENCODINGS
from ._sniff import parse_char_option
from ._util import parse_size
from .errors import OptionsError
from .infer import parse_type

TYPES = ("infer", "string", "loose")
BAD_LINES = ("error", "skip", "quarantine", "pad")
PROFILES = ("fast", "balanced", "max")
INFER = ("sample", "full")
ENGINES = ("auto", "native", "arrow", "pure")


def parse_schema(spec: Union[str, Dict[str, str], None]) -> Dict[str, str]:
    """Accept {'zip': 'string'} or 'zip=string, price=decimal(10,2)'."""
    if not spec:
        return {}
    items: Dict[str, str] = {}
    if isinstance(spec, dict):
        items = {str(k): str(v) for k, v in spec.items()}
    else:
        depth = 0
        current = ""
        parts = []
        for ch in str(spec):
            depth += (ch in "([" and 1) or (ch in ")]" and -1) or 0
            if ch == "," and depth == 0:
                parts.append(current)
                current = ""
            else:
                current += ch
        parts.append(current)
        for part in filter(None, (p.strip() for p in parts)):
            name, sep, ty = part.partition("=")
            if not sep:
                name, sep, ty = part.partition(":")
            if not sep or not name.strip():
                raise OptionsError(f"invalid schema entry {part!r}; use name=type")
            items[name.strip()] = ty.strip()
    for name, ty in items.items():
        try:
            parse_type(ty)
        except ValueError:
            raise OptionsError(f"invalid type {ty!r} for column {name!r}") from None
    return items


@dataclass
class ConvertOptions:
    to: Optional[str] = None
    source_format: Optional[str] = None
    delimiter: Optional[str] = None
    quote: Optional[str] = None
    escape: Optional[str] = None
    encoding: Optional[str] = None
    header: Optional[bool] = None
    skip_rows: Optional[int] = None
    types: str = "infer"
    schema: Dict[str, str] = field(default_factory=dict)
    null_values: List[str] = field(default_factory=list)
    bad_lines: str = "quarantine"
    quarantine: Optional[str] = None
    infer: str = "sample"
    sample_bytes: Optional[int] = None
    threads: Optional[int] = None
    memory: Optional[int] = None
    chunk_size: Optional[int] = None
    profile: str = "balanced"
    compression: Optional[str] = None
    level: Optional[int] = None
    row_group_rows: Optional[int] = None
    dictionary: Optional[bool] = None
    encodings: Dict[str, str] = field(default_factory=dict)
    tune: Optional[bool] = None
    parquet_version: Optional[str] = None
    statistics: Optional[bool] = None
    out_delimiter: Optional[str] = None
    out_header: Optional[bool] = None
    out_crlf: bool = False
    columns: Optional[List[str]] = None
    limit: Optional[int] = None
    overwrite: bool = False
    engine: str = "auto"

    @classmethod
    def build(cls, **kwargs: Any) -> ConvertOptions:
        if "from_" in kwargs:
            kwargs["source_format"] = kwargs.pop("from_")
        names = {f.name for f in fields(cls)}
        unknown = sorted(set(kwargs) - names)
        if unknown:
            raise OptionsError(f"unknown option(s): {', '.join(unknown)}")
        opts = cls(**{k: v for k, v in kwargs.items() if v is not None})
        opts.validate()
        return opts

    def validate(self) -> None:
        for name, allowed in (("types", TYPES), ("bad_lines", BAD_LINES), ("profile", PROFILES), ("infer", INFER), ("engine", ENGINES)):
            value = getattr(self, name)
            if value not in allowed:
                raise OptionsError(f"{name} must be one of {', '.join(allowed)} (got {value!r})")
        self.schema = parse_schema(self.schema)
        # dialect characters are read like the native engine reads them ('tab', '\t', 'none')
        for name in ("delimiter", "quote", "escape", "out_delimiter"):
            value = getattr(self, name)
            if value is not None:
                char = parse_char_option(str(value))
                if char == "" and name in ("delimiter", "out_delimiter"):
                    raise OptionsError(f"{name} cannot be empty")
                setattr(self, name, char)
        if isinstance(self.null_values, str):
            self.null_values = [v.strip() for v in self.null_values.split(",")]
        if isinstance(self.columns, str):
            self.columns = [c.strip() for c in self.columns.split(",") if c.strip()]
        try:
            self.memory = parse_size(self.memory, "m")
            self.chunk_size = parse_size(self.chunk_size, "m")
            self.sample_bytes = parse_size(self.sample_bytes, "m")
        except ValueError as exc:
            raise OptionsError(str(exc)) from None
        for name in ("threads", "limit", "skip_rows", "row_group_rows", "level"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, int) or isinstance(value, bool) or (value < 0 and name != "level")):
                raise OptionsError(f"{name} must be a non-negative integer")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def native_encoding(self) -> Optional[str]:
        if self.encoding is None:
            return None
        key = self.encoding.lower()
        return _NATIVE_ENCODINGS.get(key, key)

    def to_native(self, dst_fmt: str, out_delimiter: Optional[str]) -> Dict[str, Any]:
        sniff = {
            "delimiter": self.delimiter,
            "quote": self.quote,
            "escape": self.escape,
            "encoding": self.native_encoding(),
            "header": self.header,
            "skip_rows": self.skip_rows,
        }
        parquet = {
            "compression": self.compression,
            "level": self.level,
            "profile": self.profile,
            "row_group_rows": self.row_group_rows,
            "dictionary": self.dictionary,
            "encodings": self.encodings,
            "tune": self.tune,
            "statistics": self.statistics,
            "version": self.parquet_version,
        }
        out: Dict[str, Any] = {
            "from": _native_format(self.source_format) if self.source_format else None,
            "to": _native_format(dst_fmt),
            "sniff": {k: v for k, v in sniff.items() if v is not None},
            "types": self.types,
            "schema": self.schema,
            "null_values": self.null_values,
            "bad_lines": self.bad_lines,
            "quarantine": self.quarantine,
            "infer": self.infer,
            "sample_bytes": self.sample_bytes,
            "threads": self.threads,
            "memory": self.memory,
            "chunk_size": self.chunk_size,
            "parquet": {k: v for k, v in parquet.items() if v is not None},
            "compression": self.compression,
            "level": self.level,
            "profile": self.profile,
            "out_delimiter": self.out_delimiter or out_delimiter,
            "out_header": self.out_header,
            "out_crlf": self.out_crlf,
            "overwrite": self.overwrite,
            "columns": self.columns,
            "limit": self.limit,
        }
        return {k: v for k, v in out.items() if v is not None}


def _native_format(fmt: str) -> str:
    return {"tsv": "csv", "psv": "csv"}.get(fmt, fmt)
