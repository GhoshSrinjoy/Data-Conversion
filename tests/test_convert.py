import json
import math
from pathlib import Path

import pytest

import dataconv as dc
from conftest import ENGINES, have_arrow, needs_arrow, needs_native


def _schema(report):
    return dict(report.schema)


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("ext", ["jsonl", "csv", "parquet"])
def test_lossless_types_every_engine(tricky, tmp_path, engine, ext):
    if engine == "pure" and ext == "parquet":
        pytest.skip("the pure engine writes text formats")
    out = tmp_path / f"out.{ext}"
    r = dc.convert(tricky, out, engine=engine)
    assert r.rows == 2000
    assert r.bad_rows == 1
    assert r.blank_lines in (0, 1)
    if ext != "csv":
        s = _schema(r)
        assert s["zip"] == "string"
        assert s["big"] == "decimal(20,0)"
        assert s["precise"] == "decimal(24,20)"
        assert s["when"] == "timestamp[ms,+05:30]"
        assert s["day"] == "date"
        assert s["ok"] == "bool"
        assert s["price"] == "float64"
        assert any("zip" in w and "leading zeros" in w for w in r.warnings)
    assert Path(r.quarantine).read_text(encoding="utf-8").count("this,row,is,broken") == 1


@pytest.mark.parametrize("engine", ENGINES)
def test_jsonl_values(tricky, tmp_path, engine):
    out = tmp_path / "v.jsonl"
    dc.convert(tricky, out, engine=engine)
    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert rows[1]["zip"] == "07919"
    assert rows[0]["price"] is None  # NA in a numeric column is missing
    assert rows[1]["name"] == "NA"  # ...but kept in a text column
    assert rows[2]["name"] == "line\nbreak"
    assert rows[0]["note"] is None  # unquoted empty: missing
    if engine != "pure" or __import__("csv").__dict__.get("QUOTE_NOTNULL"):
        assert rows[1]["note"] == ""  # quoted empty: empty string
    assert str(rows[17]["big"]) == "18446744073709551615"


@needs_arrow
def test_parquet_values_round_trip(tricky, tmp_path):
    import pyarrow.parquet as pq

    out = tmp_path / "t.parquet"
    dc.convert(tricky, out)
    t = pq.read_table(out)
    assert t.column("zip")[1].as_py() == "07919"
    assert str(t.column("big")[17].as_py()) == "18446744073709551615"
    assert str(t.column("precise")[3].as_py()) == "1.00000000000000000001"
    back = tmp_path / "back.csv"
    dc.convert(out, back)
    again = tmp_path / "again.parquet"
    dc.convert(back, again)
    assert dc.verify(out, again)["equal"]


@needs_native
def test_recovers_from_late_type_change(tmp_path):
    src = tmp_path / "drift.csv"
    lines = ["a,b"] + [f"{i},{i % 10}" for i in range(200_000)] + ["1.5,x"]
    src.write_text("\n".join(lines) + "\n")
    r = dc.convert(src, tmp_path / "d.parquet", sample_bytes=65536, chunk_size=262144)
    assert r.restarts == 1
    assert _schema(r) == {"a": "float64", "b": "string"}
    assert r.rows == 200_001


@pytest.mark.parametrize("engine", ENGINES)
def test_late_windows_1252(tmp_path, engine):
    src = tmp_path / "latin.csv"
    body = b"city,n\n" + b"".join(b"Paris,%d\n" % i for i in range(150_000)) + b"Z\xfcrich,1\n"
    src.write_bytes(body)
    out = tmp_path / "l.jsonl"
    dc.convert(src, out, engine=engine)
    last = json.loads(out.read_text(encoding="utf-8").splitlines()[-1])
    assert last["city"] == "Zürich"


@pytest.mark.parametrize("engine", ENGINES)
def test_bad_line_policies(tmp_path, engine):
    src = tmp_path / "bad.csv"
    src.write_text("a,b,c\n1,2,3\n4,5\n6,7,8,9\n")
    with pytest.raises(dc.DataconvError):
        dc.convert(src, tmp_path / "x.jsonl", engine=engine, bad_lines="error")
    assert not (tmp_path / "x.jsonl").exists(), "no partial output on failure"
    r = dc.convert(src, tmp_path / "p.jsonl", engine=engine, bad_lines="pad")
    assert (r.rows, r.bad_rows) == (2, 1)
    r = dc.convert(src, tmp_path / "s.jsonl", engine=engine, bad_lines="skip")
    assert (r.rows, r.bad_rows, r.quarantine) == (1, 2, None)


@pytest.mark.parametrize("engine", ENGINES)
def test_options(tmp_path, engine):
    src = tmp_path / "o.csv"
    src.write_text("a;b;c\n1;x;2024-01-01\n2;y;2024-01-02\n")
    r = dc.convert(src, tmp_path / "o.jsonl", engine=engine, columns=["c", "a"], schema="a=string", limit=1)
    assert r.schema == [("c", "date"), ("a", "string")]
    assert r.rows == 1
    with pytest.raises(FileExistsError):
        dc.convert(src, tmp_path / "o.jsonl", engine=engine)
    r = dc.convert(src, tmp_path / "o.tsv", engine=engine)
    assert (tmp_path / "o.tsv").read_text().splitlines()[0] == "a\tb\tc"


def test_text_to_text_keeps_values(tmp_path):
    src = tmp_path / "keep.csv"
    src.write_text('id,zip\n1,007\n2,"a,b"\n')
    for engine in ENGINES:
        out = tmp_path / f"k-{engine}.csv"
        dc.convert(src, out, engine=engine)
        assert out.read_text().splitlines() == ["id,zip", "1,007", '2,"a,b"']


@needs_native
def test_compressed_inputs_and_outputs(tricky, tmp_path):
    gz = tmp_path / "t.csv.gz"
    dc.convert(tricky, gz)
    zst = tmp_path / "t.jsonl.zst"
    r = dc.convert(gz, zst)
    assert r.rows == 2000
    assert dc.count(zst) == 2000


def test_directory_conversion(tmp_path):
    tree = tmp_path / "tree"
    (tree / "sub").mkdir(parents=True)
    for i in range(6):
        (tree / ("sub" if i % 2 else "") / f"f{i}.csv").write_text("a,b\n1,x\n2,y\n")
    (tree / "notes.txt").write_text("not a table")
    fmt = "parquet" if (have_arrow() or "native" in ENGINES) else "jsonl"
    batch = dc.convert(tree, tmp_path / "lake", to=fmt)
    assert len(batch.reports) == 6 and not batch.failed
    assert (tmp_path / "lake" / "sub" / f"f1.{fmt}").exists()
    again = dc.convert(tree, tmp_path / "lake", to=fmt, skip_existing=True)
    assert len(again.skipped) == 6


def test_atomic_output_on_cancel(tricky, tmp_path):
    out = tmp_path / "cancel.jsonl"

    def stop(done, total):
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        dc.convert(tricky, out, progress=stop, chunk_size=4096)
    assert not out.exists()
    assert [p.name for p in tmp_path.iterdir()] == ["tricky.csv"]


def test_unknown_option():
    with pytest.raises(dc.OptionsError):
        dc.convert("x.csv", "y.parquet", nonsense=1)


def test_float_specials(tmp_path):
    src = tmp_path / "f.csv"
    src.write_text("x\n1.5\nNaN\ninf\n-inf\n")
    for engine in ENGINES:
        rows = []
        out = tmp_path / f"f-{engine}.csv"
        dc.convert(src, out, engine=engine, schema="x=float64")
        rows = out.read_text().splitlines()[1:]
        values = [float(v) for v in rows]
        assert values[0] == 1.5 and math.isnan(values[1]) and values[2] == math.inf and values[3] == -math.inf
