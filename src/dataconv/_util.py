"""Small shared helpers: sizes, durations, atomic outputs, background prefetching."""

from __future__ import annotations

import os
import queue
import re
import secrets
import shutil
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator, Optional, TypeVar, Union

from .errors import OutputExistsError

T = TypeVar("T")
PathLike = Union[str, "os.PathLike[str]"]

KiB = 1 << 10
MiB = 1 << 20
GiB = 1 << 30

_UNITS = {"": 1, "b": 1, "k": KiB, "m": MiB, "g": GiB, "t": 1 << 40, "p": 1 << 50}
_SIZE_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([kmgtp]?)(?:i?b)?\s*$", re.IGNORECASE)


def parse_size(value: Union[str, int, float, None], default_unit: str = "") -> Optional[int]:
    """Parse '64MB', '1.5g', '512k' or a number into bytes (units are powers of 1024).

    Numbers are bytes.  ``default_unit`` applies only to unit-less *strings* (the
    command line), e.g. ``parse_size("100", "m")`` is 100 MiB.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"invalid size {value!r}")
    if isinstance(value, (int, float)):
        if value < 0:
            raise ValueError(f"invalid size {value!r}")
        return int(value)
    match = _SIZE_RE.match(str(value))
    if not match:
        raise ValueError(f"invalid size {value!r}; use forms like 512K, 64MB, 1.5G")
    number, unit = match.groups()
    if not unit and not re.search(r"[bB]\s*$", str(value)):
        unit = default_unit
    return int(float(number) * _UNITS[unit.lower()[:1]])


def format_size(num: Optional[float]) -> str:
    if num is None:
        return "?"
    num = float(num)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num) < 1024 or unit == "TB":
            return f"{num:.0f} {unit}" if unit == "B" else f"{num:.1f} {unit}"
        num /= 1024
    return f"{num:.1f} PB"


def format_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms"
    if seconds < 60:
        return f"{seconds:.2f} s"
    minutes, secs = divmod(round(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def format_rate(num_bytes: float, seconds: float) -> str:
    if seconds <= 0:
        return "-"
    return f"{format_size(num_bytes / seconds)}/s"


def format_count(num: Optional[int]) -> str:
    return "?" if num is None else f"{num:,}"


def clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, value))


def _remove(path: Path) -> None:
    # On Windows a file can stay locked for a moment after its last handle is closed
    # (a virus scanner reading it): try again briefly before leaving it.
    for attempt in range(10):
        try:
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
            elif path.exists() or path.is_symlink():
                path.unlink()
            return
        except FileNotFoundError:
            return
        except OSError:
            if os.name != "nt" or attempt == 9:
                return
            time.sleep(0.05 * (attempt + 1))


def _replace(tmp: Path, final: Path) -> None:
    if tmp.is_dir() and final.exists():
        # Directories cannot be atomically replaced on every OS: swap via a backup name.
        backup = final.with_name(f".{final.name}.{secrets.token_hex(4)}.old")
        os.replace(final, backup)
        try:
            os.replace(tmp, final)
        except BaseException:
            os.replace(backup, final)
            raise
        _remove(backup)
    else:
        os.replace(tmp, final)


def check_overwrite(path: PathLike, overwrite: bool) -> Path:
    target = Path(path)
    if target.exists() and not overwrite:
        raise OutputExistsError(f"{target} already exists (pass overwrite=True / --overwrite to replace it)")
    return target


@contextmanager
def atomic_output(path: PathLike, overwrite: bool = False, directory: bool = False) -> Iterator[Path]:
    """Yield a temporary sibling path; move it over ``path`` only if the block succeeds.

    An interrupted or failed write therefore never leaves a truncated output behind.
    """
    final = check_overwrite(path, overwrite)
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.with_name(f".{final.name}.{os.getpid()}.{secrets.token_hex(4)}.tmp")
    if directory:
        tmp.mkdir()
    try:
        yield tmp
        if not tmp.exists():
            raise FileNotFoundError(f"nothing was written to {tmp}")
        _replace(tmp, final)
    except BaseException:
        _remove(tmp)
        raise


def free_disk_space(path: PathLike) -> Optional[int]:
    probe = Path(path).resolve()
    while not probe.exists() and probe.parent != probe:
        probe = probe.parent
    try:
        return shutil.disk_usage(str(probe)).free
    except OSError:
        return None


_DONE = object()


class _Failure:
    __slots__ = ("error",)

    def __init__(self, error: BaseException) -> None:
        self.error = error


def prefetch(items: Iterable[T], depth: int = 1) -> Iterator[T]:
    """Run ``items`` in a background thread, keeping up to ``depth`` results ready.

    Lets I/O and parsing of the next chunk overlap with processing of the current
    one.  Exceptions are re-raised in the consumer; abandoning the iterator stops
    the producer.
    """
    buffer: queue.Queue[object] = queue.Queue(maxsize=max(1, depth))
    stop = threading.Event()

    def produce() -> None:
        try:
            for item in items:
                while not stop.is_set():
                    try:
                        buffer.put(item, timeout=0.1)
                        break
                    except queue.Full:
                        continue
                if stop.is_set():
                    return
            final: object = _DONE
        except BaseException as exc:  # forwarded to the consumer
            final = _Failure(exc)
        while not stop.is_set():
            try:
                buffer.put(final, timeout=0.1)
                return
            except queue.Full:
                continue

    worker = threading.Thread(target=produce, name="dataconv-prefetch", daemon=True)
    worker.start()
    try:
        while True:
            item = buffer.get()
            if item is _DONE:
                return
            if isinstance(item, _Failure):
                raise item.error
            yield item  # type: ignore[misc]
    finally:
        stop.set()
        worker.join(timeout=5)
