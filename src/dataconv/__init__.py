"""dataconv: fast, lossless, resource-aware data conversion and compression.

>>> import dataconv as dc
>>> dc.convert("data.csv", "data.parquet")          # lossless types, parallel, tuned
>>> dc.compress("data.csv")                          # data.csv.dcz, byte-exact restore
>>> dc.info("data.parquet")["rows"]
>>> blob = dc.pack([1.5, 2.25, 3.0]); dc.unpack(blob)

Heavy dependencies load only when used, so ``import dataconv`` is instant.
"""

from __future__ import annotations

from typing import Any

__version__ = "1.0.0"

_API = {
    "convert": "api",
    "convert_dir": "api",
    "split": "api",
    "merge": "api",
    "load": "api",
    "write": "api",
    "to_arrow": "api",
    "compress": "api",
    "decompress": "api",
    "pack": "api",
    "unpack": "api",
    "info": "api",
    "head": "api",
    "count": "api",
    "sniff": "api",
    "schema": "api",
    "system_info": "api",
    "plan": "api",
    "verify": "_verify",
    "bench": "_bench",
    "stats": "_stats",
    "Report": "report",
    "BatchReport": "report",
    "ConvertOptions": "options",
    "DataconvError": "errors",
    "MissingDependencyError": "errors",
    "FileError": "errors",
    "FormatError": "errors",
    "SchemaError": "errors",
    "DataError": "errors",
    "IntegrityError": "errors",
    "OutputExistsError": "errors",
    "OptionsError": "errors",
}

__all__ = [*sorted(_API), "__version__"]


def __getattr__(name: str) -> Any:
    module = _API.get(name)
    if module is None:
        raise AttributeError(f"module 'dataconv' has no attribute {name!r}")
    import importlib

    value = getattr(importlib.import_module(f".{module}", __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list:
    return __all__
