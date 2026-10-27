"""Hardware, OS and container-aware resource detection, and execution planning.

Everything works without third-party packages: CPU counts honour affinity masks
and cgroup quotas (Docker, Kubernetes), memory honours cgroup limits and 32-bit
address-space limits, and each OS has a native fallback when psutil is absent.
"""

from __future__ import annotations

import math
import os
import platform
import re
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional, Tuple

from . import _compat
from ._util import GiB, MiB, clamp, parse_size

CGROUP_ROOT = "/sys/fs/cgroup"


def _read(path: str) -> Optional[str]:
    try:
        with open(path, encoding="ascii", errors="replace") as f:
            return f.read().strip()
    except OSError:
        return None


def _read_int(path: str) -> Optional[int]:
    text = _read(path)
    if text is None or not text.lstrip("-").isdigit():
        return None
    return int(text)


def cgroup_cpu_quota(root: str = CGROUP_ROOT) -> Optional[float]:
    """CPUs granted by a cgroup quota (v2 ``cpu.max`` or v1 CFS), if any."""
    text = _read(os.path.join(root, "cpu.max"))
    if text:
        parts = text.split()
        if len(parts) >= 2 and parts[0] != "max":
            try:
                return int(parts[0]) / int(parts[1])
            except (ValueError, ZeroDivisionError):
                pass
    for sub in ("cpu", "cpu,cpuacct", "cpuacct,cpu"):
        quota = _read_int(os.path.join(root, sub, "cpu.cfs_quota_us"))
        period = _read_int(os.path.join(root, sub, "cpu.cfs_period_us"))
        if quota and quota > 0 and period:
            return quota / period
    return None


def cgroup_memory(root: str = CGROUP_ROOT) -> Tuple[Optional[int], Optional[int]]:
    """(limit, usage) in bytes from cgroup v2 or v1, when a limit is set."""
    limit = _read(os.path.join(root, "memory.max"))
    if limit and limit != "max" and limit.isdigit():
        usage = _read_int(os.path.join(root, "memory.current"))
        return int(limit), usage
    v1 = _read_int(os.path.join(root, "memory", "memory.limit_in_bytes"))
    if v1 and v1 < (1 << 60):
        return v1, _read_int(os.path.join(root, "memory", "memory.usage_in_bytes"))
    return None, None


def _meminfo() -> Tuple[Optional[int], Optional[int]]:
    text = _read("/proc/meminfo")
    if not text:
        return None, None
    values: Dict[str, int] = {}
    for line in text.splitlines():
        m = re.match(r"(\w+):\s+(\d+)\s*kB", line)
        if m:
            values[m.group(1)] = int(m.group(2)) * 1024
    total = values.get("MemTotal")
    available = values.get("MemAvailable")
    if available is None and total is not None:
        available = values.get("MemFree", 0) + values.get("Buffers", 0) + values.get("Cached", 0)
    return total, available


def _windows_memory() -> Tuple[Optional[int], Optional[int]]:
    import ctypes

    class MEMORYSTATUSEX(ctypes.Structure):
        _fields_ = [
            ("dwLength", ctypes.c_ulong),
            ("dwMemoryLoad", ctypes.c_ulong),
            ("ullTotalPhys", ctypes.c_ulonglong),
            ("ullAvailPhys", ctypes.c_ulonglong),
            ("ullTotalPageFile", ctypes.c_ulonglong),
            ("ullAvailPageFile", ctypes.c_ulonglong),
            ("ullTotalVirtual", ctypes.c_ulonglong),
            ("ullAvailVirtual", ctypes.c_ulonglong),
            ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
        ]

    stat = MEMORYSTATUSEX()
    stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)):  # type: ignore[attr-defined]
        return None, None
    return int(stat.ullTotalPhys), int(min(stat.ullAvailPhys, stat.ullAvailVirtual))


def _macos_memory() -> Tuple[Optional[int], Optional[int]]:
    total = None
    try:
        total = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    except (ValueError, OSError, AttributeError):
        pass
    try:
        out = subprocess.run(["vm_stat"], capture_output=True, text=True, timeout=2).stdout
        page = int(re.search(r"page size of (\d+)", out).group(1))  # type: ignore[union-attr]
        free = 0
        for key in ("Pages free", "Pages inactive", "Pages speculative", "Pages purgeable"):
            m = re.search(key + r":\s+(\d+)", out)
            if m:
                free += int(m.group(1)) * page
        return total, free or None
    except Exception:
        return total, None


def _sysconf_memory() -> Tuple[Optional[int], Optional[int]]:
    try:
        page = os.sysconf("SC_PAGE_SIZE")
        total = os.sysconf("SC_PHYS_PAGES") * page
    except (ValueError, OSError, AttributeError):
        return None, None
    try:
        available = os.sysconf("SC_AVPHYS_PAGES") * page
    except (ValueError, OSError, AttributeError):
        available = None
    return total, available


def _cpu_counts() -> Tuple[int, Optional[int], int]:
    logical = os.cpu_count() or 1
    physical: Optional[int] = None
    psutil = _compat.optional("psutil")
    if psutil is not None:
        try:
            physical = psutil.cpu_count(logical=False)
        except Exception:
            physical = None
    usable = logical
    process_cpu_count = getattr(os, "process_cpu_count", None)
    if process_cpu_count is not None:
        usable = process_cpu_count() or usable
    elif hasattr(os, "sched_getaffinity"):
        try:
            usable = len(os.sched_getaffinity(0)) or usable
        except OSError:
            pass
    elif psutil is not None:
        try:
            usable = len(psutil.Process().cpu_affinity()) or usable
        except Exception:
            pass
    if sys.platform.startswith("linux"):
        quota = cgroup_cpu_quota()
        if quota:
            usable = min(usable, max(1, math.ceil(quota)))
    return logical, physical, max(1, usable)


def _in_container() -> bool:
    if os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv"):
        return True
    text = _read("/proc/1/cgroup") or ""
    return any(k in text for k in ("docker", "kubepods", "containerd", "lxc"))


@dataclass(frozen=True)
class SystemInfo:
    os: str
    os_release: str
    arch: str
    python: str
    implementation: str
    bits: int
    gil: bool
    cpu_logical: int
    cpu_physical: Optional[int]
    cpu_usable: int
    mem_total: int
    mem_available: int
    mem_limit: Optional[int]
    container: bool
    memory_source: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def detect() -> SystemInfo:
    logical, physical, usable = _cpu_counts()
    total = available = None
    source = "fallback"
    psutil = _compat.optional("psutil")
    if psutil is not None:
        try:
            vm = psutil.virtual_memory()
            total, available, source = int(vm.total), int(vm.available), "psutil"
        except Exception:
            pass
    if total is None:
        if sys.platform.startswith("linux"):
            total, available = _meminfo()
            source = "procfs"
        elif sys.platform == "win32":
            try:
                total, available = _windows_memory()
                source = "win32"
            except Exception:
                pass
        elif sys.platform == "darwin":
            total, available = _macos_memory()
            source = "sysctl"
        else:
            total, available = _sysconf_memory()
            source = "sysconf"
    if total is None:
        total, available, source = 4 * GiB, 1 * GiB, "fallback"
    if available is None:
        available = total // 2
    limit = None
    if sys.platform.startswith("linux"):
        limit, usage = cgroup_memory()
        if limit and limit < total:
            total = limit
            available = min(available, max(0, limit - (usage or 0)))
            source += "+cgroup"
    bits = 64 if sys.maxsize > 2**32 else 32
    if bits == 32:
        total = min(total, 3 * GiB)
        available = min(available, 3 * GiB // 2)
    return SystemInfo(
        os=platform.system().lower() or sys.platform,
        os_release=platform.release(),
        arch=platform.machine().lower(),
        python=platform.python_version(),
        implementation=platform.python_implementation(),
        bits=bits,
        gil=_compat.gil_enabled(),
        cpu_logical=logical,
        cpu_physical=physical,
        cpu_usable=usable,
        mem_total=int(total),
        mem_available=int(max(available, 0)),
        mem_limit=limit,
        container=_in_container(),
        memory_source=source,
    )


@dataclass(frozen=True)
class Plan:
    threads: int
    memory_budget: int
    chunk_size: int
    wave: int
    workers: int
    low_memory: bool
    notes: Tuple[str, ...] = field(default=())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def make_plan(
    input_size: Optional[int] = None,
    threads: Optional[int] = None,
    memory: Any = None,
    info: Optional[SystemInfo] = None,
    files: int = 1,
) -> Plan:
    """Decide threads, memory budget and chunk sizes for a job.

    ``memory`` accepts bytes or a size string ('2GB').  Environment variables
    ``DATACONV_THREADS`` and ``DATACONV_MEMORY`` override the detected values.
    """
    info = info or detect()
    notes = []
    env_threads = os.environ.get("DATACONV_THREADS")
    threads = int(threads or (env_threads and int(env_threads)) or info.cpu_usable)
    threads = max(1, threads)
    budget = parse_size(memory or os.environ.get("DATACONV_MEMORY") or None, "m")
    if budget is None:
        budget = int(min(info.mem_available * 0.5, info.mem_total * 0.8))
        notes.append(f"memory budget = 50% of available RAM ({info.mem_available // MiB} MB)")
    budget = max(budget, 64 * MiB)
    wave_bytes = int(budget / 4.5)
    chunk = clamp(wave_bytes // threads, 1 * MiB, 64 * MiB)
    if input_size:
        chunk = min(chunk, max(256 * 1024, -(-input_size // threads)))
    wave = clamp(wave_bytes // max(chunk, 1), 1, threads * 2)
    # Many small files: run several at once with fewer threads each.
    workers = 1
    if files > 1:
        per_file = (input_size or 0) / files
        workers = threads if per_file < 32 * MiB else max(1, threads // 4)
        workers = max(1, min(workers, files, int(budget // (128 * MiB)) or 1))
    low = budget < 512 * MiB
    if low:
        notes.append("low-memory mode: small chunks and row groups")
    if not info.gil:
        notes.append("free-threaded Python: pure-Python stages also run in parallel")
    return Plan(threads, budget, chunk, wave, workers, low, tuple(notes))
