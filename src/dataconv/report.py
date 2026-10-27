"""Result objects returned by conversions and compression jobs."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from ._util import format_count, format_duration, format_rate, format_size


@dataclass
class Report:
    source: str = ""
    destination: str = ""
    source_format: str = ""
    destination_format: str = ""
    engine: str = ""
    rows: int = 0
    columns: int = 0
    bytes_in: int = 0
    bytes_out: int = 0
    seconds: float = 0.0
    threads: int = 0
    memory_budget: int = 0
    chunk_size: int = 0
    schema: List[Tuple[str, str]] = field(default_factory=list)
    compression: str = ""
    dialect: Optional[Dict[str, Any]] = None
    bad_rows: int = 0
    blank_lines: int = 0
    irregular_records: int = 0
    quarantine: Optional[str] = None
    restarts: int = 0
    warnings: List[str] = field(default_factory=list)
    #: Files written by ``split`` ({"path", "rows", "bytes"} each).
    parts: List[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Report:
        known = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        if "schema" in known:
            known["schema"] = [tuple(x) for x in known["schema"]]
        return cls(**known)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @property
    def ratio(self) -> float:
        return self.bytes_in / self.bytes_out if self.bytes_out else 0.0

    @property
    def throughput(self) -> float:
        return self.bytes_in / self.seconds if self.seconds else 0.0

    def summary(self) -> str:
        what = f"{format_count(self.rows)} rows x {self.columns} cols, " if self.rows or self.columns else ""
        size = f"{format_size(self.bytes_in)} -> {format_size(self.bytes_out)}"
        if self.bytes_out and self.bytes_out <= self.bytes_in:
            size += f" ({self.ratio:.1f}x smaller)"
        elif self.bytes_out:
            size += f" ({self.bytes_out / max(1, self.bytes_in):.1f}x larger)"
        speed = f"{format_duration(self.seconds)}, {format_rate(self.bytes_in, self.seconds)}"
        extra = f"{self.engine}, {self.threads} threads" if self.threads else self.engine
        return f"{self.source} -> {self.destination}: {what}{size} in {speed} [{extra}]"

    def __str__(self) -> str:
        return self.summary()


@dataclass
class BatchReport:
    """Result of converting a directory tree."""

    reports: List[Report] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    failed: List[Tuple[str, str]] = field(default_factory=list)
    seconds: float = 0.0

    @property
    def ok(self) -> bool:
        return not self.failed

    @property
    def bytes_in(self) -> int:
        return sum(r.bytes_in for r in self.reports)

    @property
    def bytes_out(self) -> int:
        return sum(r.bytes_out for r in self.reports)

    @property
    def rows(self) -> int:
        return sum(r.rows for r in self.reports)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "reports": [r.to_dict() for r in self.reports],
            "skipped": self.skipped,
            "failed": [{"source": s, "error": e} for s, e in self.failed],
            "seconds": self.seconds,
            "rows": self.rows,
            "bytes_in": self.bytes_in,
            "bytes_out": self.bytes_out,
        }

    def summary(self) -> str:
        ratio = f" ({self.bytes_in / self.bytes_out:.1f}x)" if self.bytes_out else ""
        return (
            f"{len(self.reports)} converted, {len(self.skipped)} up to date, {len(self.failed)} failed: "
            f"{format_count(self.rows)} rows, {format_size(self.bytes_in)} -> {format_size(self.bytes_out)}{ratio} "
            f"in {format_duration(self.seconds)}"
        )

    def __str__(self) -> str:
        return self.summary()
