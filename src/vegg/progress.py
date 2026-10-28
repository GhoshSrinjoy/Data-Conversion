"""A dependency-free progress bar for terminals (silent when output is redirected)."""

from __future__ import annotations

import shutil
import sys
import time
from typing import IO, Optional

from ._util import format_duration, format_size


class Progress:
    """Callable progress reporter: ``progress(done_bytes, total_bytes)``.

    Redraws at most ten times a second and only on interactive terminals.
    """

    def __init__(self, label: str, enabled: Optional[bool] = None, stream: Optional[IO[str]] = None) -> None:
        self.label = label
        self.stream = stream or sys.stderr
        isatty = getattr(self.stream, "isatty", lambda: False)
        self.enabled = isatty() if enabled is None else enabled
        self.start = time.perf_counter()
        self.last_draw = 0.0
        self.done = 0
        self.total = 0
        self._drawn = False

    def __call__(self, done: int, total: int) -> bool:
        self.done, self.total = done, total
        now = time.perf_counter()
        if self.enabled and (now - self.last_draw >= 0.1 or (total and done >= total)):
            self.last_draw = now
            self._draw(now)
        return True

    def _draw(self, now: float) -> None:
        elapsed = max(now - self.start, 1e-9)
        rate = self.done / elapsed
        width = shutil.get_terminal_size((80, 20)).columns
        if self.total:
            frac = min(1.0, self.done / self.total)
            eta = (self.total - self.done) / rate if rate > 0 else 0.0
            stats = f" {frac * 100:5.1f}%  {format_size(self.done)}/{format_size(self.total)}  {format_size(rate)}/s  ETA {format_duration(eta)}"
        else:
            frac = 0.0
            stats = f" {format_size(self.done)}  {format_size(rate)}/s"
        bar_width = max(10, width - len(self.label) - len(stats) - 4)
        filled = int(bar_width * frac)
        bar = "#" * filled + "-" * (bar_width - filled)
        line = f"{self.label} [{bar}]{stats}"
        self.stream.write("\r" + line[: width - 1])
        self.stream.flush()
        self._drawn = True

    def close(self) -> None:
        if self.enabled and self._drawn:
            width = shutil.get_terminal_size((80, 20)).columns
            self.stream.write("\r" + " " * (width - 1) + "\r")
            self.stream.flush()

    def __enter__(self) -> Progress:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
