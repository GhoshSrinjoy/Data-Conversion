from pathlib import Path

import dataconv as dc
from conftest import needs_arrow, needs_native


def test_info_head_count_schema(tricky):
    info = dc.info(tricky, exact=True)
    assert info["format"] == "csv"
    assert info["rows"] in (2001, 2002)  # records, including the malformed one
    head = dc.head(tricky, 3)
    assert head["columns"][:3] == ["id", "zip", "big"]
    assert head["rows"][1][1] == "07919"
    assert dc.count(tricky) in (2001, 2002)
    assert dict(dc.schema(tricky))["zip"].lower() in ("string", "utf8")
    sniffed = dc.sniff(tricky)
    assert sniffed["delimiter"] == "," and sniffed["header"]


@needs_native
def test_split_byte_exact(tmp_path):
    src = tmp_path / "in.csv"
    body = "".join(f'{i},"q, {i}","multi\nline"\r\n' for i in range(9000))
    src.write_text("a,b,c\r\n" + body, newline="")
    r = dc.split(src, tmp_path / "parts", rows=2500, pattern="chunk_{n:04}")
    assert [Path(p["path"]).name for p in r.parts] == [f"chunk_{i:04}.csv" for i in range(4)]
    joined = "".join(Path(p["path"]).read_text(newline="")[len("a,b,c\r\n"):] for p in r.parts)
    assert joined == body


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
