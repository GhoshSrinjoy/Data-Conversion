"""The original scripts keep working as before."""

import subprocess
import sys
from pathlib import Path

import pytest

from conftest import have_arrow, have_native

ROOT = Path(__file__).resolve().parents[1]
needs_parquet = pytest.mark.skipif(not (have_arrow() or have_native()), reason="Parquet output needs an engine")


def run(script, *args, cwd):
    return subprocess.run([sys.executable, str(ROOT / script), *map(str, args)], capture_output=True, text=True, cwd=cwd)


@needs_parquet
def test_csv_to_parquet(tmp_path, tricky):
    proc = run("csv_to_parquet.py", tricky, cwd=tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert "Successfully converted" in proc.stdout
    assert tricky.with_suffix(".parquet").exists()
    folder = tmp_path / "many"
    folder.mkdir()
    (folder / "a.csv").write_text("x,y\n1,2\n")
    out = tmp_path / "out"
    proc = run("csv_to_parquet.py", folder, "--output", out, cwd=tmp_path)
    assert (out / "a.parquet").exists(), proc.stdout + proc.stderr


@needs_parquet
def test_splitter(tmp_path):
    src = tmp_path / "big.csv"
    src.write_text("id,v\n" + "".join(f"{i},{i * 3}\n" for i in range(200_000)))
    proc = run("csv_splitter_converter.py", src, tmp_path / "chunks", "--chunk-size", "0.2", cwd=tmp_path)
    assert proc.returncode == 0, proc.stderr
    chunks = sorted((tmp_path / "chunks").glob("chunk_*.parquet"))
    assert len(chunks) >= 2
    assert chunks[0].name == "chunk_0000.parquet"


@needs_parquet
def test_viewer(tmp_path, tricky):
    pq = tmp_path / "v.parquet"
    run("csv_to_parquet.py", tricky, "--output", pq, cwd=tmp_path)
    proc = run("parquet_viewer.py", pq, "--rows", "3", cwd=tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert "Number of rows: 2000" in proc.stdout
    assert "07919" in proc.stdout
