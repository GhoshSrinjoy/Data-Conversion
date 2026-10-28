"""Build the native release artifacts (the standalone ``vegg`` binary and the vegg
wheel with the Rust engine) without paths of the build machine in them.

Rust embeds the source path of every crate in panic messages, so a binary built in a
home directory names that directory, and with it the user.  This builds with those
paths remapped (the home directory to ``~``, the cargo home to ``cargo``, this checkout
to ``vegg``), then checks every artifact for paths of this machine that remain:

    python native/build_release.py                 # binary and wheel, into dist/
    python native/build_release.py --no-wheel      # the binary only
    python native/build_release.py --check dist/*  # only check existing artifacts

The wheel is built with maturin (``pip install maturin``); set ``MATURIN`` to use
another command, such as ``uvx maturin``.
"""

from __future__ import annotations

import argparse
import glob
import os
import re
import shlex
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Iterator, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
NATIVE = ROOT / "native"
DIST = ROOT / "dist"


def machine_paths() -> List[Tuple[Path, str]]:
    """(path, replacement) pairs, general before specific: rustc applies the last
    remapping that matches."""
    home = Path.home()
    cargo_home = Path(os.environ.get("CARGO_HOME") or home / ".cargo")
    rustup_home = Path(os.environ.get("RUSTUP_HOME") or home / ".rustup")
    return [(home, "~"), (rustup_home, "rustup"), (cargo_home, "cargo"), (ROOT, "vegg")]


def build_env() -> dict:
    env = dict(os.environ)
    if "CARGO_ENCODED_RUSTFLAGS" in env:
        flags = [f for f in env["CARGO_ENCODED_RUSTFLAGS"].split("\x1f") if f]
    else:
        flags = env.pop("RUSTFLAGS", "").split()
    # the encoded form keeps paths with spaces whole
    flags += [f"--remap-path-prefix={path}={to}" for path, to in machine_paths()]
    env["CARGO_ENCODED_RUSTFLAGS"] = "\x1f".join(flags)
    # remapped builds get a cache of their own, so they do not rebuild everyday builds
    env["CARGO_TARGET_DIR"] = str(NATIVE / "target" / "dist")
    return env


def run(cmd: List[str], cwd: Path, env: dict) -> None:
    print("+", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=cwd, env=env, check=True)


def build_binary(env: dict) -> Path:
    run(["cargo", "build", "--release", "-p", "vegg"], NATIVE, env)
    exe = ".exe" if os.name == "nt" else ""
    built = Path(env["CARGO_TARGET_DIR"]) / "release" / f"vegg{exe}"
    DIST.mkdir(exist_ok=True)
    return Path(shutil.copy2(built, DIST / built.name))


def build_wheel(env: dict) -> List[Path]:
    before = set(DIST.glob("*.whl"))
    maturin = shlex.split(os.environ.get("MATURIN", "maturin"))
    # maturin reads [tool.maturin] in the root pyproject.toml
    run([*maturin, "build", "--release", "-o", str(DIST)], ROOT, env)
    return sorted(set(DIST.glob("*.whl")) - before) or sorted(DIST.glob("vegg-*-abi3-*.whl"))


def leaks_pattern() -> re.Pattern[bytes]:
    """Any path of this machine, with either separator, in any case."""
    variants = set()
    for path, _ in machine_paths():
        text = str(path)
        for form in (text, text.replace("\\", "/"), text.replace("/", "\\")):
            variants.add(re.escape(form.encode("utf-8")))
    return re.compile(b"|".join(sorted(variants, key=len, reverse=True)), re.IGNORECASE)


def contents(path: Path) -> Iterator[Tuple[str, bytes]]:
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as z:
            for name in z.namelist():
                yield f"{path.name}!{name}", z.read(name)
    else:
        yield path.name, path.read_bytes()


def check(paths: List[Path]) -> bool:
    pattern = leaks_pattern()
    clean = True
    for path in paths:
        for name, data in contents(path):
            found = sorted({m.group(0).decode("utf-8", "replace") for m in pattern.finditer(data)})
            if found:
                clean = False
                print(f"{name}: names paths of this machine: {', '.join(found[:3])}")
    print("no paths of this machine found" if clean else "artifacts name paths of this machine")
    return clean


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--no-binary", action="store_true", help="skip the standalone binary")
    parser.add_argument("--no-wheel", action="store_true", help="skip the Python wheel")
    parser.add_argument("--check", nargs="+", metavar="FILE", help="only check these artifacts")
    args = parser.parse_args(argv)
    if args.check:
        return 0 if check([Path(p) for pattern in args.check for p in glob.glob(pattern)]) else 1
    env = build_env()
    built: List[Path] = []
    if not args.no_binary:
        built.append(build_binary(env))
    if not args.no_wheel:
        built.extend(build_wheel(env))
    return 0 if check(built) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
