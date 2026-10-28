"""The builds of vegg agree: the pure-Python wheel and the platform wheels share one
pyproject.toml, and the Rust crates carry the same version as the package."""

from pathlib import Path

import pytest

import vegg
from vegg import _compat, _native

ROOT = Path(__file__).resolve().parents[1]


def _toml(*parts: str) -> dict:
    tomllib = pytest.importorskip("tomllib")  # Python 3.11+
    path = ROOT.joinpath(*parts)
    if not path.exists():
        pytest.skip(f"{path.name} is not part of this copy of the sources")
    return tomllib.loads(path.read_text(encoding="utf-8"))


def test_versions_agree():
    project = _toml("pyproject.toml")["project"]
    crates = _toml("native", "Cargo.toml")["workspace"]["package"]
    cli_dependency = _toml("native", "cli", "Cargo.toml")["dependencies"]["vegg-core"]
    assert project["version"] == vegg.__version__ == crates["version"]
    assert cli_dependency["version"] == f"={crates['version']}"


def test_license_copies_match():
    # the crates ship their own copy, since a published crate holds only its folder
    for copy in ("native/core/LICENSE", "native/cli/LICENSE"):
        path = ROOT / copy
        if not path.exists():
            pytest.skip(f"{copy} is not part of this copy of the sources")
        assert path.read_bytes() == (ROOT / "LICENSE").read_bytes(), copy


def test_native_module_name():
    assert _toml("pyproject.toml")["tool"]["maturin"]["module-name"] == _compat.NATIVE


@pytest.mark.skipif(not _native.available(), reason="the native engine is not installed")
def test_native_engine_matches_the_package():
    assert _native.version() == vegg.__version__
