"""JSON input: lossless numbers, mixed fields, late keys and the streaming parser."""

import json
from pathlib import Path

import pytest

import dataconv as dc
from conftest import ENGINES, have_arrow, needs_arrow

PY_ENGINES = [e for e in ("arrow", "pure") if e in ENGINES]
SCHEMA = {
    "int64": "int64",
    "decimal(20,0)": "decimal",
    "float64": "float64",
    "double": "float64",
    "decimal128(20, 0)": "decimal",
    "string": "string",
    "bool": "bool",
}


def put(path, text):
    path.write_bytes(text.encode("utf-8"))


def rows(path):
    return [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines()]


def kinds(report):
    out = {}
    for name, ty in report.schema:
        t = ty.lower()
        out[name] = (
            "decimal"
            if t.startswith("decimal")
            else "float64"
            if t in ("float64", "double")
            else "string"
            if t in ("string", "utf8")
            else t
        )
    return out


@pytest.mark.parametrize("engine", PY_ENGINES)
def test_numbers_are_lossless(tmp_path, engine):
    src = tmp_path / "n.jsonl"
    put(
        src,
        "\n".join(
            [
                '{"i":1,"big":18446744073709551615,"f":1.5,"d":0.1234567890123456789,"e":1E5,"huge":1e400,"long":1'
                + "0" * 40
                + "}",
                '{"i":2,"big":1,"f":2,"d":1.5,"e":2.5e-3,"huge":2,"long":3}',
            ]
        )
        + "\n",
    )
    out = tmp_path / "n.jsonl.out.jsonl"
    r = dc.convert(src, out, engine=engine)
    assert kinds(r) == {
        "i": "int64",
        "big": "decimal",
        "f": "float64",
        "d": "decimal",
        "e": "float64",
        "huge": "string",
        "long": "string",
    }
    first, second = rows(out)
    assert first["big"] == 18446744073709551615 and second["f"] == 2.0
    assert str(first["d"]) == "0.1234567890123456789" or first["d"] == 0.1234567890123456789
    assert out.read_text().splitlines()[0].count("0.1234567890123456789") == 1  # all 19 digits written
    # numbers kept as text keep their digits (the exponent becomes e with a sign), and
    # JSON output writes them as numbers again
    line1, line2 = out.read_text().splitlines()
    assert '"huge":1e+400' in line1 and '"huge":2,' in line2
    assert first["long"] == 10**40 and '"long":1' + "0" * 40 + "}" in line1


@pytest.mark.parametrize("engine", PY_ENGINES)
def test_negative_zero_is_never_an_integer(tmp_path, engine):
    src = tmp_path / "z.jsonl"
    put(src, '{"a":-0,"b":-0}\n{"a":1.5,"b":0.12345678901234567891}\n')
    out = tmp_path / "z.out.jsonl"
    r = dc.convert(src, out, engine=engine)
    assert kinds(r) == {"a": "float64", "b": "string"}
    first = out.read_text().splitlines()[0]
    assert '"a":-0.0' in first and '"b":-0}' in first  # b holds number text: written as the number


@pytest.mark.parametrize("engine", PY_ENGINES)
def test_mixed_fields_hold_json_text(tmp_path, engine):
    src = tmp_path / "m.json"
    put(src, '[{"a":1,"o":{},"n":1.5},{"a":"1","o":{},"n":"NaN"},{"a":true,"n":"Infinity"},{"a":null,"n":"-Infinity"}]')
    out = tmp_path / "m.jsonl"
    r = dc.convert(src, out, engine=engine)
    # 1 and "1" stay different: the column holds their JSON text, written as that JSON
    assert [x["a"] for x in rows(out)] == [1, "1", True, None]
    assert "column 'a' mixes JSON types; values are stored as JSON text" in r.warnings
    assert [x["o"] for x in rows(out)] == [{}, {}, None, None]  # objects that never have a field
    csv = tmp_path / "m.csv"
    dc.convert(src, csv, engine=engine)
    assert csv.read_text().splitlines()[1:3] == ["1,{},1.5", '"""1""",{},NaN']  # CSV: the JSON text as is
    # NaN and infinities: float64 in, the strings "NaN", "Infinity", "-Infinity" out
    assert kinds(r)["n"] == "float64"
    assert [x["n"] for x in rows(out)] == [1.5, "NaN", "Infinity", "-Infinity"]


@pytest.mark.parametrize("engine", PY_ENGINES)
def test_first_seen_order_and_bom(tmp_path, engine):
    src = tmp_path / "o.jsonl"
    src.write_bytes(b'\xef\xbb\xbf{"zeta":1,"alpha":2}\n\n{"mid":3,"zeta":4}\n')
    out = tmp_path / "o.out.csv"
    r = dc.convert(src, out, engine=engine)
    assert [n for n, _ in r.schema] == ["zeta", "alpha", "mid"]
    assert out.read_bytes().splitlines()[0] == b"zeta,alpha,mid"


@pytest.mark.parametrize("engine", PY_ENGINES)
def test_numbers_cut_at_the_read_boundary(tmp_path, engine):
    values = list(range(1_000_000, 1_400_000))
    src = tmp_path / "n.json"
    put(src, json.dumps(values))
    out = tmp_path / "n.jsonl"
    r = dc.convert(src, out, engine=engine)
    assert r.rows == len(values)
    assert [x["value"] for x in rows(out)] == values


@pytest.mark.parametrize("engine", PY_ENGINES)
def test_keys_beyond_the_sample_are_kept(tmp_path, engine):
    records = [{"id": i} for i in range(100_002)] + [{"id": 1, "extra": "kept"}]
    src = tmp_path / "late.json"
    put(src, json.dumps(records))
    out = tmp_path / "late.jsonl"
    r = dc.convert(src, out, engine=engine)
    assert [n for n, _ in r.schema] == ["id", "extra"]
    assert rows(out)[-1] == {"id": 1, "extra": "kept"}
    assert r.restarts == 1


@pytest.mark.parametrize("engine", PY_ENGINES)
def test_late_values_replan(tmp_path, engine):
    src = tmp_path / "late.jsonl"
    put(src, "".join(f'{{"a":{i}}}\n' for i in range(100_002)) + '{"a":"x"}\n')
    out = tmp_path / "late.out.jsonl"
    r = dc.convert(src, out, engine=engine)
    assert kinds(r)["a"] == "string"
    assert rows(out)[0]["a"] == 0 and rows(out)[-1]["a"] == "x"


@needs_arrow
def test_nested_keys_beyond_the_first_batch(tmp_path):
    import pyarrow.parquet as pq

    records = [{"id": i, "meta": {"x": i}} for i in range(70_000)] + [{"id": 70_000, "meta": {"x": 1, "secret": "kept"}}]
    src = tmp_path / "nested.json"
    put(src, json.dumps(records))
    out = tmp_path / "nested.parquet"
    dc.convert(src, out, engine="arrow")
    assert pq.read_table(out).slice(70_000).to_pylist() == [{"id": 70_000, "meta": {"x": 1, "secret": "kept"}}]
    # beyond the 100,000-record sample: the plan widens instead of dropping the key
    records = [{"id": i, "meta": {"x": i}} for i in range(100_001)] + [{"id": 0, "meta": {"x": 1, "late": True}}]
    put(src, json.dumps(records))
    r = dc.convert(src, out, engine="arrow", overwrite=True)
    assert r.restarts == 1
    assert pq.read_table(out).slice(100_001).to_pylist() == [{"id": 0, "meta": {"x": 1, "late": True}}]


@pytest.mark.parametrize("engine", PY_ENGINES)
def test_deep_nesting_is_a_clean_error(tmp_path, engine):
    src = tmp_path / "deep.jsonl"
    put(src, '{"a":' + "[" * 100_000 + "]" * 100_000 + "}\n")
    with pytest.raises(dc.DataError):
        dc.convert(src, tmp_path / "deep.out.jsonl", engine=engine)
    put(src, '{"a":' + "[" * 200 + "]" * 200 + "}\n")
    with pytest.raises(dc.DataError):
        dc.convert(src, tmp_path / "deep2.out.jsonl", engine=engine, overwrite=True)


@pytest.mark.parametrize("engine", PY_ENGINES)
def test_invalid_json_is_a_clean_error(tmp_path, engine):
    src = tmp_path / "bad.jsonl"
    put(src, '{"a":1}\n{"a":\n')
    with pytest.raises(dc.DataError):
        dc.convert(src, tmp_path / "bad.out.jsonl", engine=engine)


@pytest.mark.parametrize("engine", ENGINES)
def test_jsonl_without_a_known_extension(tmp_path, engine):
    src = tmp_path / "events.log"
    put(src, '{"a":1,"b":"x"}\n{"a":2,"b":"y"}\n')
    out = tmp_path / "events.jsonl"
    dc.convert(src, out, engine=engine)
    assert rows(out) == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]


def test_nan_and_infinity_in_json_output(tmp_path):
    from dataconv.engines import pure

    assert pure.json_value(float("nan")) == '"NaN"'
    assert pure.json_value(float("inf")) == '"Infinity"' and pure.json_value(float("-inf")) == '"-Infinity"'
    assert pure.json_text([1.5, float("nan"), {"k": pure.JsonNumber("1E5")}]) == '[1.5,"NaN",{"k":1e+5}]'
    assert pure.number_text("-1.0E+7") == "-1.0e+7" and pure.number_text("2.5e-3") == "2.5e-3"


@pytest.mark.skipif(not have_arrow(), reason="pyarrow not installed")
def test_temporal_and_binary_text_output(tmp_path):
    import datetime as dt

    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table(
        {
            "t": pa.array([dt.time(10, 0, 1, 500000), None], pa.time64("us")),
            "d": pa.array([1500, None], pa.duration("ms")),
            "b": pa.array([b"\xff\x00", None]),
            "ns": pa.array([1704067200123456789, None], pa.timestamp("ns")),
            "utc": pa.array([0, None], pa.timestamp("s", "UTC")),
            "day": pa.array([-719528, None], pa.date32()),
        }
    )
    src = tmp_path / "t.parquet"
    pq.write_table(table, src)
    out = tmp_path / "t.jsonl"
    dc.convert(src, out, engine="arrow")
    assert rows(out)[0] == {
        "t": "10:00:01.500",
        "d": "PT1.5S",
        "b": "ff00",
        "ns": "2024-01-01T00:00:00.123456789",
        "utc": "1970-01-01T00:00:00Z",
        "day": "0000-01-01",
    }
    head = dc.head(src, 1, engine="arrow")
    assert head["rows"][0][3] == "2024-01-01T00:00:00.123456789"


@pytest.mark.parametrize("engine", ENGINES)
def test_numbers_and_nan_strings_that_float64_cannot_hold_are_text(tmp_path, engine):
    src = tmp_path / "s.jsonl"
    put(src, '{"x":1e400}\n{"x":"NaN"}\n{"x":"Infinity"}\n{"x":"-Infinity"}\n{"x":2.5E-3}\n{"x":null}\n')
    out = tmp_path / "s.out.jsonl"
    r = dc.convert(src, out, engine=engine)
    assert kinds(r) == {"x": "string"} and not r.warnings  # not a column of mixed types
    # the numbers as JSON numbers (canonical exponent), the strings as written
    assert out.read_text().splitlines() == [
        '{"x":1e+400}',
        '{"x":"NaN"}',
        '{"x":"Infinity"}',
        '{"x":"-Infinity"}',
        '{"x":2.5e-3}',
        '{"x":null}',
    ]
    csv = tmp_path / "s.csv"
    dc.convert(src, csv, engine=engine)
    assert csv.read_text().splitlines() == ["x", "1e+400", "NaN", "Infinity", "-Infinity", "2.5e-3", ""]
    # integers beyond float64 too; with numbers float64 holds, NaN strings make floats
    put(src, '{"x":12345678901234567890123}\n{"x":"NaN"}\n{"x":9007199254740993}\n')
    r = dc.convert(src, out, engine=engine, overwrite=True)
    assert kinds(r) == {"x": "string"} and not r.warnings
    assert out.read_text().splitlines() == ['{"x":12345678901234567890123}', '{"x":"NaN"}', '{"x":9007199254740993}']
    put(src, '{"x":1.5}\n{"x":"NaN"}\n')
    r = dc.convert(src, out, engine=engine, overwrite=True)
    assert kinds(r) == {"x": "float64"}


@pytest.mark.parametrize("engine", [e for e in ENGINES if e != "pure"])
@pytest.mark.parametrize("mid", ["parquet", "arrow"])
def test_json_held_as_text_round_trips_as_json(tmp_path, engine, mid):
    pa = pytest.importorskip("pyarrow")
    src = tmp_path / "in.jsonl"
    put(src, '{"v": 1e400, "m": 1, "n": [1, 2]}\n{"v": 1.5, "m": "1", "n": null}\n')
    middle = tmp_path / f"mid.{mid}"
    dc.convert(src, middle, engine=engine)
    if mid == "parquet":
        import pyarrow.parquet as pq

        schema = pq.read_schema(middle)
    else:
        schema = pa.ipc.open_file(pa.OSFile(str(middle))).schema
    assert schema.field("v").metadata == {b"dataconv.json": b"number"}
    assert schema.field("m").metadata == {b"dataconv.json": b"value"}
    back = tmp_path / "back.jsonl"
    dc.convert(middle, back, engine=engine)
    assert back.read_text() == '{"v":1e+400,"m":1,"n":[1,2]}\n{"v":1.5,"m":"1","n":null}\n'


@needs_arrow
@pytest.mark.parametrize("engine", [e for e in ("native", "arrow") if e in ENGINES])
def test_json_marks_on_nested_fields(tmp_path, engine):
    import pyarrow as pa
    import pyarrow.parquet as pq

    number = {b"dataconv.json": b"number"}
    value = {b"dataconv.json": b"value"}
    struct = pa.struct([pa.field("x", pa.string(), metadata=number), pa.field("y", pa.string())])
    schema = pa.schema(
        [
            pa.field("s", struct),
            pa.field("l", pa.list_(pa.field("item", pa.string(), metadata=value))),
            pa.field("t", pa.string(), metadata=number),
        ]
    )
    table = pa.table(
        {
            "s": [{"x": "1e+400", "y": "7"}, {"x": "NaN", "y": None}],
            "l": [["1", '"a"', "{}"], None],
            "t": ["-0", "Infinity"],
        },
        schema=schema,
    )
    src = tmp_path / "marked.parquet"
    pq.write_table(table, src)
    out = tmp_path / "marked.jsonl"
    dc.convert(src, out, engine=engine)
    assert out.read_text().splitlines() == [
        '{"s":{"x":1e+400,"y":"7"},"l":[1,"a",{}],"t":-0}',
        '{"s":{"x":"NaN","y":null},"l":null,"t":"Infinity"}',
    ]
    csv = tmp_path / "marked.csv"
    dc.convert(src, csv, engine=engine)
    assert csv.read_text().splitlines()[1:] == [
        '"{""x"":1e+400,""y"":""7""}","[1,""a"",{}]",-0',
        '"{""x"":""NaN"",""y"":null}",,Infinity',
    ]


@pytest.mark.parametrize("engine", ENGINES)
def test_nested_values_follow_the_lossless_rules(tmp_path, engine):
    # objects are structs (a missing key is null), arrays lists, and every leaf follows
    # the rules of a column: no number turns into a float that loses it
    src = tmp_path / "n.jsonl"
    put(
        src,
        '{"l":[1e400],"o":{"x":0.1234567890123456789},"f":[1,2.5],"s":{"x":1},"m":{"k":1}}\n'
        '{"l":[2],"o":{"x":1.5},"f":[3],"s":{"x":2,"y":"z"},"m":{"k":"1"}}\n',
    )
    out = tmp_path / "n.out.jsonl"
    r = dc.convert(src, out, engine=engine)
    assert out.read_text().splitlines() == [
        '{"l":[1e+400],"o":{"x":0.1234567890123456789},"f":[1.0,2.5],"s":{"x":1,"y":null},"m":{"k":1}}',
        '{"l":[2],"o":{"x":1.5000000000000000000},"f":[3.0],"s":{"x":2,"y":"z"},"m":{"k":"1"}}',
    ]
    assert r.schema == [
        ("l", "list<string>"),
        ("o", "struct<x: decimal(20,19)>"),  # 1 integer digit (1.5) and 19 fraction digits
        ("f", "list<float64>"),
        ("s", "struct<x: int64, y: string>"),
        ("m", "struct<k: string>"),
    ]
    assert r.warnings == ["column 'm.k' mixes JSON types; values are stored as JSON text"]
    h = dc.head(src, 2, engine=engine)
    assert h["types"] == [t for _, t in r.schema] and h["rows"][0][2] == "[1.0,2.5]"
    # head plans from the records it shows, as the native engine's does
    assert dc.head(src, 1, engine=engine)["types"][1:5:3] == ["struct<x: decimal(19,19)>", "struct<k: int64>"]
    assert dc.head(src, 0, engine=engine) == {"columns": [], "types": [], "rows": []}


@pytest.mark.parametrize("engine", [e for e in ENGINES if e != "pure"])
def test_nested_json_parquet_schema(tmp_path, engine):
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    src = tmp_path / "n.jsonl"
    put(src, '{"o":{"x":1e400,"y":[1,"a"],"z":{}},"l":[[1.5]]}\n{"o":null,"l":[]}\n')
    out = tmp_path / "n.parquet"
    dc.convert(src, out, engine=engine)
    schema = pq.read_schema(out)
    o = schema.field("o").type
    assert pa.types.is_struct(o) and [o.field(i).name for i in range(o.num_fields)] == ["x", "y", "z"]
    assert o.field("x").metadata == {b"dataconv.json": b"number"}
    assert o.field("y").type.value_field.metadata == {b"dataconv.json": b"value"}
    assert o.field("z").metadata == {b"dataconv.json": b"value"}  # an object that never has a field
    assert pq.read_table(out).to_pylist()[0]["o"] == {"x": "1e+400", "y": ["1", '"a"'], "z": "{}"}


@pytest.mark.parametrize("engine", ENGINES)
def test_json_lines_are_read_as_the_native_engine_reads_them(tmp_path, engine):
    src = tmp_path / "w.jsonl"
    out = tmp_path / "w.out.jsonl"
    # a lone CR is JSON whitespace inside a record: only a line feed ends one
    put(src, '{"a":1,\r"b":2}\n\n \x0c\n{"a":3,"b":4}\n')
    dc.convert(src, out, engine=engine)
    assert rows(out) == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    for text, what in [
        ('{"a":1}\r{"a":2}\n', "invalid JSON in the line at byte 0"),
        ('\xa0{"a":1}\n', "invalid JSON in the line at byte 0"),
        ('\x0c{"a":1}\n', "invalid JSON in the line at byte 0"),
        ('\x1e{"a":1}\n', "invalid JSON in the line at byte 0"),
        ('{"a":1}\n[1,2,3]\n', "the JSON Lines record at byte 8 is not an object"),
        ('{"a":1}\n"x"\n', "the JSON Lines record at byte 8 is not an object"),
        ('{"a":NaN}\n{"a":1.5}\n', "invalid JSON in the line at byte 0"),
        ('{"a":-Infinity}\n', "invalid JSON in the line at byte 0"),
    ]:
        put(src, text)
        with pytest.raises(dc.DataError, match=what.replace("[", r"\[")):
            dc.convert(src, out, engine=engine, overwrite=True)
    # a lone surrogate cannot be text: a clean error, and no output
    put(src, '{"a":"\\ud83d"}\n')
    out.unlink()
    with pytest.raises(dc.DataError, match="does not fit Utf8"):
        dc.convert(src, out, engine=engine)
    assert not out.exists()


@pytest.mark.parametrize("engine", PY_ENGINES)
def test_a_number_cut_by_the_read_boundary_of_an_array(tmp_path, engine):
    body = "[ " + "0.5," * 300000 + "0.5]"
    assert body[(1 << 20) - 1] == "."  # the first read ends right after a '.'
    src = tmp_path / "nums.json"
    put(src, body)
    out = tmp_path / "nums.jsonl"
    r = dc.convert(src, out, engine=engine)
    assert r.rows == 300001 and set(out.read_text().splitlines()) == {'{"value":0.5}'}


@pytest.mark.parametrize("engine", ENGINES)
def test_late_json_values_replan_as_the_native_engine_says(tmp_path, engine):
    src = tmp_path / "late.jsonl"
    put(src, "".join(f'{{"a":{i},"o":{{"x":1}}}}\n' for i in range(100_001)) + '{"a":1.5,"o":{"x":2,"y":"z"}}\n')
    out = tmp_path / "late.out.jsonl"
    r = dc.convert(src, out, engine=engine, sample_bytes="64KB")
    assert kinds(r) == {"a": "float64", "o": "struct<x: int64, y: string>"}
    assert r.restarts == 1
    assert r.warnings == ["column 'a': value 1.5 does not fit Int64; re-planned from a full scan"]
    assert rows(out)[-1] == {"a": 1.5, "o": {"x": 2, "y": "z"}} and rows(out)[0] == {"a": 0.0, "o": {"x": 1, "y": None}}
