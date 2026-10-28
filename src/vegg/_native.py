"""Bridge to the Rust engine (the ``vegg._rust`` extension module).

vegg's platform wheels include the engine; the pure-Python wheel, used where no
platform wheel fits (Python 3.8, PyPy, other platforms), and source installs do not.

The engine exchanges options and reports as JSON, so this module only has to
load it, check that its protocol matches, and translate its errors into the
exceptions of :mod:`vegg.errors`.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, Optional

from . import _compat
from .errors import (
    DataError,
    FileError,
    FormatError,
    IntegrityError,
    OptionsError,
    OutputExistsError,
    SchemaError,
    VeggError,
)

PROTOCOL = 1

# Why the engine can be missing, for errors and `vegg doctor`.
MISSING = ("the native engine is not part of this installation; vegg's wheels for CPython 3.9+ "
           "on Windows, macOS and Linux include it")

_ERRORS = {
    "io": FileError,
    "format": FormatError,
    "data": DataError,
    "schema": SchemaError,
    "integrity": IntegrityError,
    "exists": OutputExistsError,
    "invalid": OptionsError,
}

_checked: Dict[int, bool] = {}


def module() -> Optional[Any]:
    """The native module when installed, enabled and protocol-compatible."""
    mod = _compat.optional(_compat.NATIVE)
    if mod is None:
        return None
    key = id(mod)
    if key not in _checked:
        try:
            _checked[key] = mod.protocol() == PROTOCOL
        except Exception:
            _checked[key] = False
    return mod if _checked[key] else None


def available() -> bool:
    return module() is not None


def version() -> Optional[str]:
    mod = module()
    return None if mod is None else str(mod.version())


def call(name: str, *args: Any, **kwargs: Any) -> Any:
    mod = module()
    if mod is None:
        raise VeggError("the native engine is not available")
    try:
        return getattr(mod, name)(*args, **kwargs)
    except mod.NativeError as exc:
        kind, message = exc.args if len(exc.args) == 2 else ("", str(exc))
        raise _ERRORS.get(kind, VeggError)(message) from None


def call_json(name: str, *args: Any, **kwargs: Any) -> Any:
    return json.loads(call(name, *args, **kwargs))


def options_json(options: Dict[str, Any]) -> str:
    return json.dumps({k: v for k, v in options.items() if v is not None})


ProgressFn = Optional[Callable[[int, int], Any]]
