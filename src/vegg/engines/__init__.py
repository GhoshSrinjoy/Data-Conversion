"""Engine selection.

``native`` (Rust, in vegg's platform wheels) is the fastest; ``arrow`` uses pyarrow; ``pure``
needs nothing but the standard library.  ``auto`` picks the first that supports
both formats on this machine.  Formats are checked before availability, so pyarrow
(slow to import) is imported only for jobs that would use it.
"""

from __future__ import annotations

from typing import List, Optional

from .. import _compat, _native, formats
from ..errors import MissingDependencyError, OptionsError

ORDER = ("native", "arrow", "pure")


def available(engine: str) -> bool:
    if engine == "native":
        return _native.available()
    if engine == "arrow":
        return _compat.have("pyarrow")
    return engine == "pure"


def installed() -> List[str]:
    return [engine for engine in ORDER if available(engine)]


def missing(engine: str) -> MissingDependencyError:
    """The error for an engine that is not installed."""
    if engine == "native":
        return MissingDependencyError(_native.MISSING)
    return MissingDependencyError(f'the {engine} engine is not installed (pip install "vegg[arrow]")')


def unsupported(engine: str, src_fmt: str, dst_fmt: str, src_comp: Optional[str], dst_comp: Optional[str]) -> str:
    """Why an installed engine cannot do a job."""
    if formats.engine_supports(engine, src_fmt, dst_fmt, None, None):
        if src_comp and not formats.engine_supports(engine, src_fmt, dst_fmt, src_comp, None):
            return f"the {engine} engine cannot read {src_comp}-compressed input (use engine='auto')"
        return f"the {engine} engine cannot write {dst_comp}-compressed {dst_fmt} (use engine='auto')"
    return f"the {engine} engine cannot convert {src_fmt} to {dst_fmt}"


def choose(engine: str, src_fmt: str, dst_fmt: str, src_comp: Optional[str] = None, dst_comp: Optional[str] = None) -> str:
    if engine != "auto":
        if not available(engine):
            raise missing(engine)
        if not formats.engine_supports(engine, src_fmt, dst_fmt, src_comp, dst_comp):
            raise OptionsError(unsupported(engine, src_fmt, dst_fmt, src_comp, dst_comp))
        return engine
    for candidate in ORDER:
        if formats.engine_supports(candidate, src_fmt, dst_fmt, src_comp, dst_comp) and available(candidate):
            return candidate
    needs = "pyarrow" if formats.FORMATS[src_fmt].arrow and formats.FORMATS[dst_fmt].arrow else "an engine"
    raise MissingDependencyError(
        f"converting {src_fmt} to {dst_fmt} needs {needs}: pip install \"vegg[arrow]\""
    )
