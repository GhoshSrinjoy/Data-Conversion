"""Bridge to the optional Rust engine (the ``dataconv-native`` package).

The engine exchanges options and reports as JSON, so this module only has to
load it, check that its protocol matches, and translate its errors into the
exceptions of :mod:`dataconv.errors`.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, Optional

from . import _compat
from .errors import (
    DataconvError,
    DataError,
    FileError,
    FormatError,
    IntegrityError,
    OptionsError,
    OutputExistsError,
    SchemaError,
)

PROTOCOL = 1

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
    mod = _compat.optional("dataconv_native")
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
        raise DataconvError("the native engine is not available")
    try:
        return getattr(mod, name)(*args, **kwargs)
    except mod.NativeError as exc:
        kind, message = exc.args if len(exc.args) == 2 else ("", str(exc))
        raise _ERRORS.get(kind, DataconvError)(message) from None


def call_json(name: str, *args: Any, **kwargs: Any) -> Any:
    return json.loads(call(name, *args, **kwargs))


def options_json(options: Dict[str, Any]) -> str:
    return json.dumps({k: v for k, v in options.items() if v is not None})


ProgressFn = Optional[Callable[[int, int], Any]]
