"""Engine selection.

``native`` (Rust, in vegg's platform wheels) is the fastest; ``arrow`` uses pyarrow; ``pure``
needs nothing but the standard library.  ``auto`` picks the first that supports
both formats on this machine.
"""

from __future__ import annotations

from typing import List, Optional

from .. import _compat, _native, formats
from ..errors import MissingDependencyError, OptionsError


def installed() -> List[str]:
    found = []
    if _native.available():
        found.append("native")
    if _compat.have("pyarrow"):
        found.append("arrow")
    found.append("pure")
    return found


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
    have = installed()
    if engine != "auto":
        if engine not in have:
            raise missing(engine)
        if not formats.engine_supports(engine, src_fmt, dst_fmt, src_comp, dst_comp):
            raise OptionsError(unsupported(engine, src_fmt, dst_fmt, src_comp, dst_comp))
        return engine
    for candidate in have:
        if formats.engine_supports(candidate, src_fmt, dst_fmt, src_comp, dst_comp):
            return candidate
    needs = "pyarrow" if formats.FORMATS[src_fmt].arrow and formats.FORMATS[dst_fmt].arrow else "an engine"
    raise MissingDependencyError(
        f"converting {src_fmt} to {dst_fmt} needs {needs}: pip install \"vegg[arrow]\""
    )
