"""Write THIRD_PARTY_LICENSES.md: the license of every crate compiled into the
native engine and the standalone binary, with the license texts each crate ships.
Identical texts are printed once, followed by the crates that use them.

Run from anywhere:  python native/scripts/third_party_licenses.py
"""

import json
import pathlib
import re
import subprocess
import sys
from collections import OrderedDict

NATIVE = pathlib.Path(__file__).resolve().parents[1]
LICENSE_FILES = ("LICENSE", "LICENCE", "COPYING", "NOTICE", "UNLICENSE")


def license_texts(root: pathlib.Path):
    for d in (root, root / "zstd", root / "xz", root / "liblzma"):
        if d.is_dir():
            for f in sorted(d.iterdir()):
                if f.is_file() and f.name.upper().startswith(LICENSE_FILES):
                    yield f.read_text(encoding="utf-8", errors="replace").strip()


def main() -> int:
    meta = json.loads(subprocess.check_output(["cargo", "metadata", "--format-version", "1", "--locked"], cwd=NATIVE))
    members = set(meta["workspace_members"])
    packages = {p["id"]: p for p in meta["packages"]}
    nodes = {n["id"]: n for n in meta["resolve"]["nodes"]}
    seen, stack = set(), list(members)
    while stack:
        pid = stack.pop()
        if pid in seen:
            continue
        seen.add(pid)
        for dep in nodes[pid]["deps"]:
            kinds = {k["kind"] for k in dep["dep_kinds"]}
            if None in kinds or "build" in kinds:  # build scripts compile the C sources we ship
                stack.append(dep["pkg"])
    bundled = sorted((packages[p] for p in seen - members), key=lambda p: (p["name"], p["version"]))

    texts: OrderedDict[str, list] = OrderedDict()
    for p in bundled:
        label = f"{p['name']} {p['version']}"
        found = False
        for text in license_texts(pathlib.Path(p["manifest_path"]).parent):
            key = re.sub(r"\s+", " ", text)
            texts.setdefault(key, [text, []])[1].append(label)
            found = True
        if not found:
            texts.setdefault(f"(license field only: {p.get('license')})", [f"Licensed under: {p.get('license')}", []])[1].append(label)

    out = [
        "# Third-party licenses",
        "",
        "The native engine (`vegg._rust`, in vegg's platform wheels) and the standalone",
        "`vegg` binary include the open-source components below. Regenerate this file",
        "with `python native/scripts/third_party_licenses.py`.",
        "",
        "| component | version | license |",
        "|-----------|---------|---------|",
    ]
    out += [f"| {p['name']} | {p['version']} | {p.get('license') or 'see texts below'} |" for p in bundled]
    out += ["", "## License texts", ""]
    for i, (text, users) in enumerate(texts.values(), 1):
        out.append(f"### Text {i}")
        out.append("")
        out.append("Used by: " + ", ".join(sorted(set(users))))
        out.append("")
        out.append("```text")
        out.append(text)
        out.append("```")
        out.append("")
    # shipped in the wheels (license-files in pyproject.toml) and next to the binaries
    with open(NATIVE / "THIRD_PARTY_LICENSES.md", "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(out) + "\n")
    print(f"wrote THIRD_PARTY_LICENSES.md: {len(bundled)} components, {len(texts)} distinct license texts")
    return 0


if __name__ == "__main__":
    sys.exit(main())
