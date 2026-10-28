"""Optional-dependency discovery.

Every accelerator vegg can use is optional.  Imports are lazy and cached so
``import vegg`` stays fast, and any of them can be switched off with the
``VEGG_DISABLE`` environment variable: comma-separated module names, ``native``
for the Rust engine, or ``all`` for the Rust engine and every third-party
package.  That switch is how the pure-Python fallbacks are exercised on machines
that do have the accelerators installed.
"""

from __future__ import annotations

import importlib
import os
import sys
import threading
from types import ModuleType
from typing import Dict, Optional

from .errors import MissingDependencyError

# Third-party packages vegg knows how to use, mapped to their pip names.
THIRD_PARTY = {
    "pyarrow": "pyarrow",
    "numpy": "numpy",
    "zstandard": "zstandard",
    "pyzstd": "pyzstd",
    "lz4": "lz4",
    "brotli": "brotli",
    "brotlicffi": "brotlicffi",
    "psutil": "psutil",
    "orjson": "orjson",
    "pandas": "pandas",
    "polars": "polars",
    "openpyxl": "openpyxl",
    "charset_normalizer": "charset-normalizer",
}

# The Rust engine, part of vegg's platform wheels; VEGG_DISABLE=native turns it off.
NATIVE = "vegg._rust"

_lock = threading.Lock()
_cache: Dict[str, Optional[ModuleType]] = {}


def _disabled() -> set:
    raw = os.environ.get("VEGG_DISABLE", "")
    return {part.strip().lower() for part in raw.split(",") if part.strip()}


def is_disabled(name: str) -> bool:
    disabled = _disabled()
    if name == NATIVE:
        return "native" in disabled or "all" in disabled
    root = name.split(".", 1)[0].lower()
    if name.lower() in disabled or root in disabled:
        return True
    return "all" in disabled and root in THIRD_PARTY


def optional(name: str) -> Optional[ModuleType]:
    """Import ``name`` if it is installed and not disabled, else return None."""
    try:
        return _cache[name]
    except KeyError:
        pass
    module: Optional[ModuleType] = None
    if not is_disabled(name):
        try:
            module = importlib.import_module(name)
        except Exception:  # ImportError, or a broken binary wheel raising anything else
            module = None
    with _lock:
        _cache[name] = module
    return module


def have(name: str) -> bool:
    return optional(name) is not None


def require(name: str, purpose: str, extra: str = "fast") -> ModuleType:
    """Import ``name`` or raise a MissingDependencyError explaining how to get it."""
    module = optional(name)
    if module is None:
        pip_name = THIRD_PARTY.get(name.split(".", 1)[0], name)
        raise MissingDependencyError(
            f"{purpose} needs the optional package '{pip_name}'. "
            f'Install it with: pip install "vegg[{extra}]"  (or: pip install {pip_name})'
        )
    return module


def reset_cache() -> None:
    """Forget cached imports (used by tests that toggle VEGG_DISABLE)."""
    with _lock:
        _cache.clear()


def version_tuple(text: str) -> tuple:
    parts = []
    for piece in text.split(".")[:3]:
        digits = ""
        for ch in piece:
            if not ch.isdigit():
                break
            digits += ch
        parts.append(int(digits or 0))
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts)


def pyarrow_version() -> tuple:
    pa = optional("pyarrow")
    return version_tuple(pa.__version__) if pa is not None else (0, 0, 0)


def gil_enabled() -> bool:
    """False on free-threaded CPython builds, where threads scale for Python code."""
    probe = getattr(sys, "_is_gil_enabled", None)
    return True if probe is None else bool(probe())


def features() -> Dict[str, Optional[str]]:
    """Installed optional packages and their versions (None when unavailable)."""
    found: Dict[str, Optional[str]] = {}
    for name in sorted(THIRD_PARTY):
        module = optional(name)
        if module is None:
            found[name] = None
        else:
            found[name] = str(getattr(module, "__version__", getattr(module, "VERSION", "installed")))
    zstd_std = optional("compression.zstd")
    found["compression.zstd (stdlib)"] = "built-in" if zstd_std is not None else None
    for name in ("lzma", "bz2", "zlib"):
        found[f"{name} (stdlib)"] = "built-in" if optional(name) is not None else None
    return found
