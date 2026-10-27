import json
import subprocess
import sys

import pytest

from dataconv.cli import main


def run(args, capsys):
    code = main([str(a) for a in args])
    out = capsys.readouterr()
    return code, out.out, out.err


def test_help_and_version(capsys):
    assert run([], capsys)[0] == 2
    with pytest.raises(SystemExit):
        main(["--version"])


def test_convert_info_head(tricky, tmp_path, capsys):
    out = tmp_path / "t.jsonl"
    code, stdout, stderr = run(["convert", tricky, out], capsys)
    assert code == 0 and out.exists()
    assert "leading zeros" in stderr
    code, stdout, _ = run(["convert", tricky, out, "--json"], capsys)
    assert code == 1  # exists
    code, stdout, _ = run(["convert", tricky, out, "-f", "--json", "--columns", "id,zip"], capsys)
    assert code == 0 and json.loads(stdout)["columns"] == 2
    code, stdout, _ = run(["info", tricky, "--json"], capsys)
    assert json.loads(stdout)["format"] == "csv"
    code, stdout, _ = run(["head", tricky, "-n", "2"], capsys)
    assert "07919" in stdout
    code, stdout, _ = run(["count", tricky], capsys)
    assert int(stdout) >= 2000
    code, stdout, _ = run(["schema", tricky, "--json"], capsys)
    assert any(c["name"] == "zip" for c in json.loads(stdout))
    code, stdout, _ = run(["sniff", tricky], capsys)
    assert "delimiter" in stdout


def test_compress_decompress(tricky, tmp_path, capsys):
    dcz = tmp_path / "t.dcz"
    assert run(["compress", tricky, dcz], capsys)[0] == 0
    back = tmp_path / "back.csv"
    assert run(["decompress", dcz, back], capsys)[0] == 0
    assert back.read_bytes() == tricky.read_bytes()
    code, stdout, _ = run(["verify", dcz, tricky], capsys)
    assert code == 0 and "identical" in stdout


def test_errors_are_reported(tmp_path, capsys):
    code, _, err = run(["convert", tmp_path / "missing.csv", tmp_path / "x.parquet"], capsys)
    assert code == 1 and "error" in err


def test_doctor(capsys):
    code, stdout, _ = run(["doctor"], capsys)
    assert code == 0 and "engines" in stdout
    code, stdout, _ = run(["doctor", "--json"], capsys)
    assert json.loads(stdout)["cpu_usable"] >= 1


def test_module_entry_point(tricky):
    proc = subprocess.run([sys.executable, "-m", "dataconv", "count", str(tricky)], capture_output=True, text=True)
    assert proc.returncode == 0 and int(proc.stdout) >= 2000
