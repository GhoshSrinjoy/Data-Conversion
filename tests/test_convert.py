import json
import math
from pathlib import Path

import pytest

import vegg
from conftest import ENGINES, have_arrow, needs_arrow, needs_native


def _schema(report):
    return dict(report.schema)


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize("ext", ["jsonl", "csv", "parquet"])
def test_lossless_types_every_engine(tricky, tmp_path, engine, ext):
    if engine == "pure" and ext == "parquet":
        pytest.skip("the pure engine writes text formats")
    out = tmp_path / f"out.{ext}"
    r = vegg.convert(tricky, out, engine=engine)
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
    vegg.convert(tricky, out, engine=engine)
    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert rows[1]["zip"] == "07919"
    assert rows[0]["price"] is None  # NA in a numeric column is missing
    assert rows[1]["name"] == "NA"  # ...but kept in a text column
    assert rows[2]["name"] == "line\nbreak"
    assert rows[0]["note"] is None  # unquoted empty: missing
    assert rows[1]["note"] == ""  # quoted empty: empty string (on every Python version)
    assert str(rows[17]["big"]) == "18446744073709551615"


@needs_arrow
def test_parquet_values_round_trip(tricky, tmp_path):
    import pyarrow.parquet as pq

    out = tmp_path / "t.parquet"
    vegg.convert(tricky, out)
    t = pq.read_table(out)
    assert t.column("zip")[1].as_py() == "07919"
    assert str(t.column("big")[17].as_py()) == "18446744073709551615"
    assert str(t.column("precise")[3].as_py()) == "1.00000000000000000001"
    back = tmp_path / "back.csv"
    vegg.convert(out, back)
    again = tmp_path / "again.parquet"
    vegg.convert(back, again)
    assert vegg.verify(out, again)["equal"]


@needs_native
def test_recovers_from_late_type_change(tmp_path):
    src = tmp_path / "drift.csv"
    lines = ["a,b"] + [f"{i},{i % 10}" for i in range(200_000)] + ["1.5,x"]
    src.write_text("\n".join(lines) + "\n")
    r = vegg.convert(src, tmp_path / "d.parquet", sample_bytes=65536, chunk_size=262144)
    assert r.restarts == 1
    assert _schema(r) == {"a": "float64", "b": "string"}
    assert r.rows == 200_001


@pytest.mark.parametrize("engine", ENGINES)
def test_late_windows_1252(tmp_path, engine):
    src = tmp_path / "latin.csv"
    body = b"city,n\n" + b"".join(b"Paris,%d\n" % i for i in range(150_000)) + b"Z\xfcrich,1\n"
    src.write_bytes(body)
    out = tmp_path / "l.jsonl"
    vegg.convert(src, out, engine=engine)
    last = json.loads(out.read_text(encoding="utf-8").splitlines()[-1])
    assert last["city"] == "Zürich"


@pytest.mark.parametrize("engine", ENGINES)
def test_bad_line_policies(tmp_path, engine):
    src = tmp_path / "bad.csv"
    src.write_text("a,b,c\n1,2,3\n4,5\n6,7,8,9\n")
    with pytest.raises(vegg.VeggError):
        vegg.convert(src, tmp_path / "x.jsonl", engine=engine, bad_lines="error")
    assert not (tmp_path / "x.jsonl").exists(), "no partial output on failure"
    r = vegg.convert(src, tmp_path / "p.jsonl", engine=engine, bad_lines="pad")
    assert (r.rows, r.bad_rows) == (2, 1)
    r = vegg.convert(src, tmp_path / "s.jsonl", engine=engine, bad_lines="skip")
    assert (r.rows, r.bad_rows, r.quarantine) == (1, 2, None)


@pytest.mark.parametrize("engine", ENGINES)
def test_options(tmp_path, engine):
    src = tmp_path / "o.csv"
    src.write_text("a;b;c\n1;x;2024-01-01\n2;y;2024-01-02\n")
    r = vegg.convert(src, tmp_path / "o.jsonl", engine=engine, columns=["c", "a"], schema="a=string", limit=1)
    assert r.schema == [("c", "date"), ("a", "string")]
    assert r.rows == 1
    with pytest.raises(FileExistsError):
        vegg.convert(src, tmp_path / "o.jsonl", engine=engine)
    r = vegg.convert(src, tmp_path / "o.tsv", engine=engine)
    assert (tmp_path / "o.tsv").read_text().splitlines()[0] == "a\tb\tc"


def test_text_to_text_keeps_values(tmp_path):
    src = tmp_path / "keep.csv"
    src.write_text('id,zip\n1,007\n2,"a,b"\n')
    for engine in ENGINES:
        out = tmp_path / f"k-{engine}.csv"
        vegg.convert(src, out, engine=engine)
        assert out.read_text().splitlines() == ["id,zip", "1,007", '2,"a,b"']


@needs_native
def test_compressed_inputs_and_outputs(tricky, tmp_path):
    gz = tmp_path / "t.csv.gz"
    vegg.convert(tricky, gz)
    zst = tmp_path / "t.jsonl.zst"
    r = vegg.convert(gz, zst)
    assert r.rows == 2000
    assert vegg.count(zst) == 2000


def test_directory_conversion(tmp_path):
    tree = tmp_path / "tree"
    (tree / "sub").mkdir(parents=True)
    for i in range(6):
        (tree / ("sub" if i % 2 else "") / f"f{i}.csv").write_text("a,b\n1,x\n2,y\n")
    (tree / "notes.txt").write_text("not a table")
    fmt = "parquet" if (have_arrow() or "native" in ENGINES) else "jsonl"
    batch = vegg.convert(tree, tmp_path / "lake", to=fmt)
    assert len(batch.reports) == 6 and not batch.failed
    assert (tmp_path / "lake" / "sub" / f"f1.{fmt}").exists()
    again = vegg.convert(tree, tmp_path / "lake", to=fmt, skip_existing=True)
    assert len(again.skipped) == 6


def test_atomic_output_on_cancel(tricky, tmp_path):
    out = tmp_path / "cancel.jsonl"

    def stop(done, total):
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        vegg.convert(tricky, out, progress=stop, chunk_size=4096)
    assert not out.exists()
    assert [p.name for p in tmp_path.iterdir()] == ["tricky.csv"]


def test_unknown_option():
    with pytest.raises(vegg.OptionsError):
        vegg.convert("x.csv", "y.parquet", nonsense=1)


def test_float_specials(tmp_path):
    src = tmp_path / "f.csv"
    src.write_text("x\n1.5\nNaN\ninf\n-inf\n")
    for engine in ENGINES:
        rows = []
        out = tmp_path / f"f-{engine}.csv"
        vegg.convert(src, out, engine=engine, schema="x=float64")
        rows = out.read_text().splitlines()[1:]
        values = [float(v) for v in rows]
        assert values[0] == 1.5 and math.isnan(values[1]) and values[2] == math.inf and values[3] == -math.inf


@pytest.mark.parametrize("engine", ENGINES)
def test_floats_are_written_alike_by_every_engine(tmp_path, engine):
    # the shortest digits that read back to the same double, plain from 1e-5 up to
    # 1e16 and otherwise 1e-7 / 1.5e22
    src = tmp_path / "f.csv"
    src.write_bytes(b"f\n1e-7\n1.5E22\n0.000015\n1e16\n1e15\n0.1\n-0.0\n5e-324\n1.7976931348623157e308\n0.0001\n")
    texts = ["1e-7", "1.5e22", "0.000015", "1e16", "1000000000000000.0", "0.1", "-0.0", "5e-324", "1.7976931348623157e308", "0.0001"]
    r = vegg.convert(src, tmp_path / "out.jsonl", engine=engine)
    assert dict(r.schema)["f"] == "float64"
    assert (tmp_path / "out.jsonl").read_text().splitlines() == [f'{{"f":{t}}}' for t in texts]
    # (CSV to CSV keeps each field's text as written)
    vegg.convert(tmp_path / "out.jsonl", tmp_path / "out.csv", engine=engine)
    assert (tmp_path / "out.csv").read_text().splitlines() == ["f", *texts]


def test_float_notation_matches_the_native_writers():
    from vegg.engines import pure

    doubles = (1e-5, 1.5e-5, 1e-4, 1e16, 123.0, -1e-7, 1e22, 1.2345678901234568e17)
    assert [pure.float_repr(v) for v in doubles] == [
        "0.00001", "0.000015", "0.0001", "1e16", "123.0", "-1e-7", "1e22", "1.2345678901234568e17"
    ]
    # float32 from its own shortest digits, in any notation: plain from 1e-6 up to 1e13
    singles = ("1", "-0", "1e+10", "1e+13", "2.5e-39", "0.00001", "1e-6", "1e-7", "123456790", "3.4028235e+38")
    assert [pure.single_repr(s) for s in singles] == [
        "1.0", "-0.0", "10000000000.0", "1e13", "2.5e-39", "0.00001", "0.000001", "1e-7", "123456790.0", "3.4028235e38"
    ]


@needs_arrow
@pytest.mark.parametrize("engine", [e for e in ("native", "arrow") if e in ENGINES])
def test_foreign_column_types_are_written_alike(tmp_path, engine):
    # types our readers never produce, as other tools write them
    import pyarrow as pa

    day = 19_724 * 86_400_000  # 2024-01-02
    columns = {
        "f32": pa.array([0.1, 3.4028235e38, None], pa.float32()),
        "f16": pa.array([1.0, None, float("nan")], pa.float32()).cast(pa.float16()),
        "d64": pa.array([day, day + 11_045_006, None], pa.int64()).cast(pa.date64()),
        "n": pa.nulls(3),
        "m": pa.array([[("a", 1), ("a", None)], None, []], pa.map_(pa.string(), pa.int64())),
        "k": pa.array([[(1, "x")], [], None], pa.map_(pa.int64(), pa.string())),
        "ts": pa.array([[1_700_000_000_123_456_789], None, []], pa.list_(pa.timestamp("ns"))),
        "st": pa.array([{"t": 1_704_144_845}, None, {"t": None}], pa.struct([("t", pa.timestamp("s", tz="+05:30"))])),
    }
    src = tmp_path / "foreign.arrow"
    table = pa.table(columns)
    with pa.ipc.new_file(pa.OSFile(str(src), "wb"), table.schema) as w:
        w.write_table(table)
    out = tmp_path / "foreign.jsonl"
    vegg.convert(src, out, engine=engine)
    assert out.read_text().splitlines() == [
        '{"f32":0.1,"f16":1.0,"d64":"2024-01-02","n":null,"m":{"a":1,"a":null},"k":{"1":"x"},'
        '"ts":["2023-11-14T22:13:20.123456789"],"st":{"t":"2024-01-02T03:04:05+05:30"}}',
        '{"f32":3.4028235e38,"f16":null,"d64":"2024-01-02T03:04:05.006","n":null,"m":null,"k":{},"ts":null,"st":null}',
        '{"f32":null,"f16":"NaN","d64":null,"n":null,"m":{},"k":null,"ts":[],"st":{"t":null}}',
    ]
    out = tmp_path / "foreign.csv"
    vegg.convert(src, out, engine=engine)
    assert out.read_text().splitlines() == [
        "f32,f16,d64,n,m,k,ts,st",
        '0.1,1.0,2024-01-02,,"{""a"":1,""a"":null}","{""1"":""x""}","[""2023-11-14T22:13:20.123456789""]",'
        '"{""t"":""2024-01-02T03:04:05+05:30""}"',
        "3.4028235e38,,2024-01-02T03:04:05.006,,,{},,",
        ',NaN,,,{},,[],"{""t"":null}"',
    ]


@pytest.mark.parametrize("engine", ENGINES)
def test_timestamps_keep_nanoseconds_and_old_years(tmp_path, engine):
    src = tmp_path / "t.csv"
    src.write_bytes(b"id,when,day\n1,2024-01-01T00:00:00.123456789,0000-01-01\n2,2024-01-02T00:00:00.000000001,2024-02-29\n")
    out = tmp_path / "t.jsonl"
    r = vegg.convert(src, out, engine=engine)
    assert dict(r.schema)["when"] == "timestamp[ns]"
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert [x["when"] for x in rows] == ["2024-01-01T00:00:00.123456789", "2024-01-02T00:00:00.000000001"]
    assert rows[0]["day"] == "0000-01-01"


@pytest.mark.parametrize("engine", ENGINES)
def test_wide_decimals(tmp_path, engine):
    src = tmp_path / "d.csv"
    src.write_bytes(b"id,amount\n1,12345678901234567890123456789\n2,1\n")
    out = tmp_path / "d.jsonl"
    r = vegg.convert(src, out, engine=engine)
    assert dict(r.schema)["amount"] == "decimal(29,0)"
    assert '"amount":12345678901234567890123456789' in out.read_text()


@needs_arrow
def test_arrow_float_checks_every_value():
    import pyarrow as pa

    from vegg import infer
    from vegg.engines import arrow

    f64 = infer.LogicalType("float64")
    for v in ("1e400", "1e-400", "0.12345678901234567891"):
        with pytest.raises(arrow.Violation):
            arrow.cast_column(pa.array(["1.5", v]), f64, "infer", [], 0)
    assert arrow.cast_column(pa.array(["1.5", "1e300", "-0"]), f64, "infer", [], 0).to_pylist() == [1.5, 1e300, -0.0]
    # an empty chunk has nothing to violate
    assert len(arrow.cast_column(pa.array([], pa.string()), infer.LogicalType("int64"), "infer", [], 0)) == 0
    with pytest.raises(arrow.Violation):  # -0 is not an integer
        arrow.cast_column(pa.array(["1", "-0"]), infer.LogicalType("int64"), "infer", [], 0)
    with pytest.raises(arrow.Violation):  # leading zeros and -0 do not fit a decimal
        arrow.cast_column(pa.array(["1.5", "007.5"]), infer.LogicalType("decimal", 5, 1), "infer", [], 0)


@needs_arrow
def test_excel_columns_mixing_kinds(tmp_path):
    openpyxl = pytest.importorskip("openpyxl")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["id", "code"])
    ws.append([1, "A1"])
    ws.append([2, 15])
    wb.save(tmp_path / "m.xlsx")
    out = tmp_path / "m.csv"
    r = vegg.convert(tmp_path / "m.xlsx", out, engine="arrow")
    assert dict(r.schema)["code"] == "string"
    assert out.read_bytes() == b"id,code\n1,A1\n2,15\n"
    with pytest.raises(vegg.OptionsError):
        vegg.convert(tmp_path / "m.xlsx", tmp_path / "p.csv", engine="pure")


def test_native_gets_the_detected_format(tmp_path):
    from vegg.engines import native

    data = native.options(vegg.ConvertOptions.build(), "jsonl", "parquet")
    assert data["from"] == "jsonl" and data["to"] == "parquet"
    assert native.options(vegg.ConvertOptions.build(), "tsv", "csv")["sniff"]["delimiter"] == "\t"
