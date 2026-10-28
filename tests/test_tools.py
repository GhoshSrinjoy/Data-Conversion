from pathlib import Path

import pytest

import dataconv as dc
from conftest import ENGINES, have_native, needs_arrow, needs_native
from dataconv import _native


def test_info_head_count_schema(tricky):
    info = dc.info(tricky, exact=True)
    assert info["format"] == "csv"
    assert info["rows"] in (2001, 2002)  # records, including the malformed one
    head = dc.head(tricky, 3)
    assert head["columns"][:3] == ["id", "zip", "big"]
    assert head["rows"][1][1] == "07919"
    assert dc.count(tricky) in (2001, 2002)
    assert dict(dc.schema(tricky))["zip"] == "string"
    sniffed = dc.sniff(tricky)
    assert sniffed["delimiter"] == "," and sniffed["header"]
    with pytest.raises(dc.OptionsError):
        dc.head(tricky, -1)


@pytest.mark.parametrize("engine", ENGINES)
def test_schema_and_head_alike_on_every_engine(tmp_path, engine):
    # the same type names, and values as CSV output writes them
    src = tmp_path / "c.csv"
    src.write_bytes(b"i,s,f,d,ts,b\n1,x,1.5E22,2024-01-02,2024-01-02T03:04:05.5+05:30,true\n,,1e-7,,,\n")
    names = ["int64", "string", "float64", "date", "timestamp[ms,+05:30]", "bool"]
    assert dc.schema(src, engine=engine) == list(zip(["i", "s", "f", "d", "ts", "b"], names))
    assert dc.head(src, 5, engine=engine)["rows"] == [
        ["1", "x", "1.5e22", "2024-01-02", "2024-01-02T03:04:05.5+05:30", "true"],
        [None, None, "1e-7", None, None, None],
    ]


@needs_arrow
@pytest.mark.parametrize("engine", [e for e in ("native", "arrow") if e in ENGINES])
def test_schema_and_head_of_foreign_types(tmp_path, engine):
    import pyarrow as pa

    table = pa.table(
        {
            "u": pa.array([1], pa.uint8()),
            "f": pa.array([0.1], pa.float32()),
            "l": pa.array([[1, None]], pa.list_(pa.int64())),
            "st": pa.array([{"a": "x"}], pa.struct([("a", pa.string())])),
            "m": pa.array([[("k", 1.5)]], pa.map_(pa.string(), pa.float64())),
            "t": pa.array([1], pa.timestamp("ns", tz="UTC")),
        }
    )
    src = tmp_path / "foreign.arrow"
    with pa.ipc.new_file(pa.OSFile(str(src), "wb"), table.schema) as w:
        w.write_table(table)
    names = ["uint8", "float32", "list<int64>", "struct<a: string>", "map<string, float64>", "timestamp[ns,UTC]"]
    assert dc.schema(src, engine=engine) == list(zip(table.column_names, names))
    assert dc.head(src, 5, engine=engine)["rows"] == [
        ["1", "0.1", "[1,null]", '{"a":"x"}', '{"k":1.5}', "1970-01-01T00:00:00.000000001Z"]
    ]


@needs_arrow
@pytest.mark.parametrize("engine", [e for e in ("native", "arrow") if e in ENGINES])
def test_parquet_info_alike(tmp_path, engine):
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({"s": ["x"], "u": pa.array([1], pa.uint32()), "t": pa.array([1], pa.timestamp("ms", tz="UTC")), "i": [1]})
    src = tmp_path / "t.parquet"
    pq.write_table(table, src, compression="zstd", compression_level=9)
    info = dc.info(src, engine=engine)
    assert [c["logical_type"] for c in info["columns"]] == ["string", "uint32", "timestamp[ms,utc]", None]
    assert info["columns"][0]["compression"] == ["zstd"]  # files do not store levels
    assert info["format_version"] == 2 and info["metadata_keys"] == ["ARROW:schema"]


@needs_native
def test_split_byte_exact(tmp_path):
    src = tmp_path / "in.csv"
    body = "".join(f'{i},"q, {i}","multi\nline"\r\n' for i in range(9000))
    src.write_bytes(("a,b,c\r\n" + body).encode())
    r = dc.split(src, tmp_path / "parts", rows=2500, pattern="chunk_{n:04}")
    assert [Path(p["path"]).name for p in r.parts] == [f"chunk_{i:04}.csv" for i in range(4)]
    joined = "".join(Path(p["path"]).read_bytes().decode()[len("a,b,c\r\n"):] for p in r.parts)
    assert joined == body


def test_part_names_and_patterns():
    from dataconv.api import check_pattern, part_name

    assert part_name("part-{n:05}", 7) == "part-00007"
    assert part_name("p-{n:05}-{n:03}", 7) == "p-00007-007"
    assert part_name("p{n}", 12) == "p12"
    assert part_name("p{n:5}", 12) == "p12"
    assert part_name("p-", 3) == "p-00003"
    # anything else in braces is text, in both engines
    assert part_name("{n{n}}", 5) == "{n5}"
    assert part_name("{nn}-{n:}-{n:5x}", 5) == "{nn}-{n:}-{n:5x}00005"
    assert len(part_name("{n:0999999999999999999999}", 1)) == 255
    check_pattern("part-{n:05}", ".csv")
    for pattern, ext in (("sub/part-{n}", ".csv"), ("part-{n}", "/x"), ("a\x01b-{n}", ".csv"), ("p-{n:0300}", ".csv")):
        with pytest.raises(dc.OptionsError):
            check_pattern(pattern, ext)


@pytest.mark.parametrize("native", [True, False] if have_native() else [False])
def test_split_names_every_placeholder(tmp_path, monkeypatch, native):
    if not native:
        monkeypatch.setattr(_native, "module", lambda: None)
    src = tmp_path / "in.csv"
    src.write_bytes(b"a\n1\n2\n3\n")
    r = dc.split(src, tmp_path / "parts", rows=2, pattern="p-{n:03}-{n}")
    assert [Path(p["path"]).name for p in r.parts] == ["p-000-0.csv", "p-001-1.csv"]
    with pytest.raises(dc.OptionsError):
        dc.split(src, tmp_path / "other", rows=2, pattern="p\x01{n}")


@needs_native
def test_split_by_size(tmp_path):
    src = tmp_path / "in.csv"
    src.write_text("id,v,name\n" + "".join(f"{i},{i % 1000}.{i % 7},name{i % 5000}\n" for i in range(300_000)))
    target = 200 * 1024
    r = dc.split(src, tmp_path / "pq", size=target, to="parquet")
    assert r.rows == 300_000 and len(r.parts) >= 3
    for p in r.parts[:-1]:
        assert 0.6 < p["bytes"] / target < 1.4


@needs_arrow
def test_merge_unifies_schemas(tmp_path):
    a = tmp_path / "a.csv"
    b = tmp_path / "b.csv"
    a.write_text("id,x\n1,1.5\n2,2.5\n")
    b.write_text("id,y\n3,hello\n")
    r = dc.merge([a, b], tmp_path / "m.parquet")
    assert r.rows == 3
    assert [n for n, _ in r.schema] == ["id", "x", "y"]


@needs_arrow
def test_verify(tricky, tmp_path):
    pq = tmp_path / "t.parquet"
    dc.convert(tricky, pq)
    back = tmp_path / "t.csv"
    dc.convert(pq, back)
    assert dc.verify(pq, back)["equal"]
    other = tmp_path / "o.csv"
    other.write_text(tricky.read_text(encoding="utf-8").replace("user7,", "user8,", 1), encoding="utf-8")
    res = dc.verify(tricky, other)
    assert not res["equal"] and res["differences"]
    dcz_file = tmp_path / "t.dcz"
    dc.compress(tricky, dcz_file)
    assert dc.verify(dcz_file, tricky)["identical_bytes"]


@needs_arrow
def test_stats(tricky):
    stats = {s["column"]: s for s in dc.stats(tricky)}
    assert stats["id"]["min"] == 0 and stats["id"]["max"] == 1999
    assert stats["price"]["nulls"] > 0
    assert stats["ok"]["distinct"] == 2


@needs_arrow
def test_load_and_write(tmp_path, tricky):
    table = dc.load(tricky)
    assert table.num_rows == 2000
    out = tmp_path / "w.parquet"
    dc.write(table, out, profile="max")
    assert dc.count(out) == 2000
    dc.write([{"a": 1, "b": "x"}, {"a": 2, "b": None}], tmp_path / "rows.jsonl")
    assert dc.count(tmp_path / "rows.jsonl") == 2


@needs_arrow
def test_bench_quick(tricky):
    result = dc.bench(tricky, quick=True)
    assert result["recommendations"]["smallest"]
    assert all("error" not in r for r in result["results"]), [r for r in result["results"] if "error" in r]


def test_system_info_and_plan():
    info = dc.system_info()
    assert info["cpu_usable"] >= 1 and info["mem_total"] > 0
    assert "pure" in info["engines"]
    plan = dc.plan("10GB")
    assert plan["threads"] >= 1 and plan["memory_budget"] > 0


def test_split_without_native_is_byte_exact(tmp_path, no_native):
    src = tmp_path / "in.csv"
    body = 'id,price,when,flag\r\n1,1.50,2024-01-01 10:00:00,True\r\n2,1e5,"x\r\ny",False\r\nbad\r\n3,2.25,z,True\r\n'
    src.write_bytes(body.encode())
    r = dc.split(src, tmp_path / "parts", rows=2)
    parts = [Path(p["path"]).read_bytes() for p in r.parts]
    header = b"id,price,when,flag\r\n"
    assert all(p.startswith(header) for p in parts)
    # records copied verbatim (the malformed one too), values untouched
    assert b"".join(p[len(header) :] for p in parts) == body.encode()[len(header) :]
    assert r.rows == 4 and [p["rows"] for p in r.parts] == [2, 2]
    assert not [p for p in (tmp_path / "parts").iterdir() if p.name.startswith(".")]  # no staging left
    with pytest.raises(FileExistsError):
        dc.split(src, tmp_path / "parts", rows=2)


@needs_arrow
def test_split_without_native_reports_rejects(tmp_path, no_native):
    src = tmp_path / "s.csv"
    src.write_bytes(b"a,b\n1,2\n3,4,5\n6,7\n8\n9,10\n")
    r = dc.split(src, tmp_path / "parts", rows=2, to="jsonl")
    assert (r.rows, r.bad_rows) == (3, 2)
    assert r.quarantine == str(tmp_path / "parts.rejected.csv")
    assert Path(r.quarantine).read_bytes() == b"a,b\n3,4,5\n8\n"
    assert any("2 malformed rows moved to" in w for w in r.warnings)


@needs_arrow
def test_verify_is_lossless(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    csv = tmp_path / "zip.csv"
    csv.write_bytes(b"id,zip,x\n1,007,1.5\n2,010,2.0\n")
    pq.write_table(pa.table({"id": [1, 2], "zip": [7, 10], "x": pa.array([1.5, 2.0], pa.float32())}), tmp_path / "p.parquet")
    for a, b in ((csv, tmp_path / "p.parquet"), (tmp_path / "p.parquet", csv)):
        res = dc.verify(a, b)
        assert not res["equal"]
        assert {d["column"] for d in res["differences"] if d["kind"] == "value"} == {"zip"}


@needs_arrow
def test_load_arrow_streams_and_options(tmp_path):
    import pyarrow as pa

    t = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    with pa.OSFile(str(tmp_path / "s.arrow"), "wb") as f, pa.ipc.new_stream(f, t.schema) as w:
        w.write_table(t)
    with pa.OSFile(str(tmp_path / "f.arrow"), "wb") as f, pa.ipc.new_file(f, t.schema) as w:
        w.write_table(t)
    assert dc.load(tmp_path / "s.arrow").equals(t)
    assert dc.load(tmp_path / "f.arrow", columns=["a"], limit=2).to_pydict() == {"a": [1, 2]}
    r = dc.convert(tmp_path / "s.arrow", tmp_path / "s.csv", engine="arrow")
    assert r.rows == 3
