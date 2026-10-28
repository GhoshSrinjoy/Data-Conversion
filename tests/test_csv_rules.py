"""The CSV rules every engine shares: quotes, blank lines, malformed rows, rejected-row
files, header names and the sniffer."""

import io
import json
from pathlib import Path

import pytest

import dataconv as dc
from conftest import ENGINES, needs_arrow
from dataconv import _csvtok, _sniff
from dataconv.engines import pure

BS = "\\"


def put(path, text):
    path.write_bytes(text.encode("utf-8"))  # no newline translation (Windows)


def rows(path):
    return [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines()]


def tokens(text, **kw):
    d = _csvtok.Dialect(**kw)
    return [(values, flags) for values, _, _, _, flags in _csvtok.tokenize(text, d)]


def test_quote_opens_a_field_only_at_its_start():
    # S3: a quote inside an unquoted field is a literal character
    assert tokens('1,55" screen,9\n') == [(["1", '55" screen', "9"], 0)]
    assert tokens('a, "b"\n') == [(["a", ' "b"'], 0)]
    # doubled quotes, an empty quoted field (not missing), lenient text after a quote
    assert tokens('"say ""hi""",""\n') == [(['say "hi"', ""], 0)]
    assert tokens('"12" monitor,x\n') == [(["12 monitor", "x"], _csvtok.IRREGULAR)]
    # escape dialects: an escaped quote is literal, the first other quote closes
    assert tokens(f'"a{BS}"b",c\n', escape=BS, double_quote=False) == [(['a"b', "c"], 0)]
    # an escape outside quotes is a literal character
    assert tokens(f"a{BS},b\n", escape=BS, double_quote=False) == [([f"a{BS}", "b"], 0)]
    # every line break kind ends a record
    assert [v for v, _ in tokens("a\nb\r\nc\rd")] == [["a"], ["b"], ["c"], ["d"]]


def test_record_cuts_follow_the_quote_rules():
    d = _csvtok.Dialect(text=False)
    assert _csvtok.last_record_end(b'1,55" screen,9\n2,x\n', d) == 19
    assert _csvtok.scan_end(b'a,b\n1,"x\ny', d)[:2] == (4, 6)
    esc = _csvtok.Dialect(escape=BS, double_quote=False, text=False)
    data = b'1,"he said \\"hi"\n2,"multi\nline"\n3,x\n'
    first = data.index(b"\n") + 1
    assert _csvtok.last_record_end(data, esc) == len(data)
    # an escaped quote does not open or close anything: no cut inside the multi-line field
    assert _csvtok.last_record_end(data[: data.index(b"line")], esc) == first


class _Slow(io.RawIOBase):
    def __init__(self, data):
        self.data, self.pos = data, 0

    def readable(self):
        return True

    def readinto(self, b):
        n = min(len(b), 3, len(self.data) - self.pos)
        b[:n] = self.data[self.pos : self.pos + n]
        self.pos += n
        return n


def test_chunks_end_at_records_and_strays_end_at_their_line():
    d = _csvtok.Dialect(text=False)
    data = b'id,v\n1,"a\nb"\n2,"open\n3,ok\n'
    chunks = list(_csvtok.Chunker(_Slow(data), 4, d))
    assert b"".join(chunks) == data
    records = [(v, flags) for c in chunks for v, _, _, _, flags in _csvtok.tokenize(c, d)]
    # S4: the never-closed quote makes its own line a malformed record, and 3,ok survives
    assert records[2] == ([b"2", b"open\n"], _csvtok.IRREGULAR | _csvtok.UNTERMINATED)
    assert records[3] == ([b"3", b"ok"], 0)


@pytest.mark.parametrize("engine", ENGINES)
def test_stray_quotes_are_malformed_rows(tmp_path, engine):
    src = tmp_path / "stray.csv"
    put(src, 'id,a,note\n1,x,ok\n10,a,"note10\n11,b,fine\n15,b,note"15\n16,d,end\n17,e,"unclosed\n18,f,last\n')
    out = tmp_path / "s.jsonl"
    r = dc.convert(src, out, engine=engine)
    assert [x["id"] for x in rows(out)] == [1, 16, 18]
    assert r.bad_rows == 2
    # S4b: the pairing of a stray quote with a later one spans lines 10-15 (3 lines)
    assert any("2 malformed rows (4 lines) moved to" in w for w in r.warnings), r.warnings
    rejected = Path(r.quarantine).read_text(encoding="utf-8")
    assert rejected == 'id,a,note\n10,a,"note10\n11,b,fine\n15,b,note"15\n17,e,"unclosed\n'
    assert dc.count(src, engine=engine) == 5  # records: rows and malformed ones
    assert r.irregular_records == 2


@pytest.mark.parametrize("engine", ENGINES)
def test_one_line_lenient_quotes_stay_values(tmp_path, engine):
    src = tmp_path / "l.csv"
    put(src, 'id,item\n1,"12" monitor\n2,x\n')
    out = tmp_path / "l.jsonl"
    r = dc.convert(src, out, engine=engine)
    assert rows(out)[0]["item"] == "12 monitor"
    assert r.bad_rows == 0 and r.irregular_records == 1


@pytest.mark.parametrize("engine", ENGINES)
def test_a_literal_quote_does_not_change_types(tmp_path, engine):
    src = tmp_path / "tv.csv"
    put(src, 'id,size,price\n1,55" screen,499.99\n2,65 inch,799.50\n')
    r = dc.convert(src, tmp_path / "tv.jsonl", engine=engine)
    assert dict(r.schema) == {"id": "int64", "size": "string", "price": "float64"}
    assert r.warnings == []


@pytest.mark.parametrize("engine", ENGINES)
def test_blank_lines(tmp_path, engine):
    # S2: in a single-column file a blank line is a missing value, elsewhere it is skipped
    one = tmp_path / "one.csv"
    put(one, 'x\na\n\n""\nb\n')
    out = tmp_path / "one.jsonl"
    r = dc.convert(one, out, engine=engine)
    assert [x["x"] for x in rows(out)] == ["a", None, "", "b"]
    assert dc.count(one, engine=engine) == r.rows == 4
    wide = tmp_path / "wide.csv"
    put(wide, "a,b\n1,2\n\n3,4\r\n\r\n")
    r = dc.convert(wide, tmp_path / "wide.jsonl", engine=engine)
    assert (r.rows, r.blank_lines) == (2, 2)
    assert dc.count(wide, engine=engine) == 2


@pytest.mark.parametrize("engine", ENGINES)
def test_header_with_a_quoted_line_break(tmp_path, engine):
    src = tmp_path / "h.csv"
    put(src, '"first\nname",age\nann,3\nbob,4\n')
    out = tmp_path / "h.jsonl"
    r = dc.convert(src, out, engine=engine)
    assert [n for n, _ in r.schema] == ["first\nname", "age"]
    assert rows(out) == [{"first\nname": "ann", "age": 3}, {"first\nname": "bob", "age": 4}]


@pytest.mark.parametrize("engine", ENGINES)
def test_header_only_file_without_a_line_break(tmp_path, engine):
    src = tmp_path / "h.csv"
    put(src, "a,b,c")
    r = dc.convert(src, tmp_path / "h.jsonl", engine=engine)
    assert r.rows == 0 and [n for n, _ in r.schema] == ["a", "b", "c"]


@pytest.mark.parametrize("engine", ENGINES)
def test_bool_columns_with_missing_values(tmp_path, engine):
    src = tmp_path / "b.csv"
    put(src, "id,flag\n1,true\n2,\n3,NA\n4,false\n")
    out = tmp_path / "b.jsonl"
    r = dc.convert(src, out, engine=engine)
    assert dict(r.schema)["flag"] == "bool"
    assert [x["flag"] for x in rows(out)] == [True, None, None, False]


@pytest.mark.parametrize("engine", ENGINES)
def test_loose_integers_with_a_plus_sign(tmp_path, engine):
    src = tmp_path / "l.csv"
    put(src, "a\n+5\n007\n-3\n")
    out = tmp_path / "l.jsonl"
    r = dc.convert(src, out, engine=engine, types="loose")
    assert dict(r.schema)["a"] == "int64"
    assert [x["a"] for x in rows(out)] == [5, 7, -3]


@pytest.mark.parametrize("engine", ENGINES)
def test_no_quoting_keeps_empty_fields_missing(tmp_path, engine):
    src = tmp_path / "n.csv"
    put(src, 'a,b\nx,\n,"y"\n')
    out = tmp_path / "n.jsonl"
    dc.convert(src, out, engine=engine, quote="none")
    assert rows(out) == [{"a": "x", "b": None}, {"a": None, "b": '"y"'}]


@pytest.mark.parametrize("engine", ENGINES)
def test_latin1_is_iso_8859_1(tmp_path, engine):
    src = tmp_path / "l.csv"
    src.write_bytes(b"name,v\nA\x80B\xe9,1\n")
    out = tmp_path / "l.jsonl"
    dc.convert(src, out, engine=engine, encoding="latin-1")
    assert rows(out)[0]["name"] == "A\u0080B\u00e9"


def test_latin1_reaches_the_native_engine_as_latin1():
    for name in ("latin-1", "latin1", "ISO-8859-1", "iso8859_1"):
        assert dc.ConvertOptions.build(encoding=name).native_encoding() == "latin-1"
    assert dc.ConvertOptions.build(encoding="cp1252").native_encoding() == "windows-1252"


@needs_arrow
def test_arrow_chunks_with_escapes_and_utf16(tmp_path):
    from dataconv.engines import arrow

    lines = ["id,text"] + [f'{i},"he said {BS}"hi{BS}" {i}"' if i % 7 == 0 else f'{i},"multi\nline {i}"' for i in range(300)]
    src = tmp_path / "e.csv"
    put(src, "\n".join(lines) + "\n")
    o = dc.ConvertOptions.build()
    d = pure._dialect(src, "csv", o)
    assert d.escape == BS
    reader = arrow.CsvReader(src, "csv", d, o, chunk_size=64)  # many chunk boundaries
    values = [v for t in reader.tables() for v in t.column("text").to_pylist()]
    assert len(values) == 300 and not reader.bad
    assert values[1] == "multi\nline 1" and values[7] == 'he said "hi" 7'
    u16 = tmp_path / "u.csv"
    u16.write_bytes(("id,name\n" + "".join(f"{i},é{i}\n" for i in range(500))).encode("utf-16"))
    d = pure._dialect(u16, "csv", o)
    reader = arrow.CsvReader(u16, "csv", d, o, chunk_size=64)
    names = [v for t in reader.tables() for v in t.column("name").to_pylist()]
    assert names[:2] == ["é0", "é1"] and len(names) == 500


def test_sniffer_rules(tmp_path):
    # S1: a column of times is one column, not ':'-separated values
    s = _sniff.sniff_bytes(b"time\n10:30\n11:45\n12:00:05.5\n", True)
    assert s.delimiter == "," and s.columns == ["time"]
    s = _sniff.sniff_bytes(b"2024-01-01 10:00:00\n2024-01-02 11:00:00\n2024-01-03 12:00:00\n", True)
    assert s.delimiter == "," and len(s.columns) == 1
    assert _sniff.sniff_bytes(b"a:b:c\n1:2:3\n4:5:6\n", True).delimiter == ":"
    # S9: names stay as written; only blank ones are generated
    s = _sniff.sniff_bytes(b' a ,"b ",,  ,a\n1,2,3,4,5\n', True)
    assert s.columns == [" a ", "b ", "column_3", "column_4", "a"]
    assert _sniff.unique_names(["x", "x", " x"], 3) == ["x", "x_2", " x"]
    # a whole-file sample splits a never-closed quote like a conversion does (its line
    # is one record); from a sample cut off by its size the unterminated record is dropped
    s = _sniff.sniff_bytes(b'"a,b\n1,2\n3,4\n5,6\n', True)
    assert (len(s.columns), s.skip_rows) == (2, 1)
    assert len(_sniff.sniff_bytes(b'"a,b\n1,2\n3,4\n5,6\n', False).columns) == 1


def test_dialect_options_are_single_characters():
    assert dc.ConvertOptions.build(delimiter="\\t", out_delimiter="tab", quote="none").to_dict()["delimiter"] == "\t"
    o = dc.ConvertOptions.build(out_delimiter="\\t")
    assert o.out_delimiter == "\t"
    with pytest.raises(dc.OptionsError):
        dc.ConvertOptions.build(delimiter=";;")
    with pytest.raises(dc.OptionsError):
        dc.ConvertOptions.build(out_delimiter="none")


@pytest.mark.parametrize("engine", ENGINES)
def test_out_delimiter_names(tmp_path, engine):
    src = tmp_path / "c.csv"
    put(src, "a,b\n1,2\n")
    out = tmp_path / "c.txt"
    dc.convert(src, out, engine=engine, to="csv", out_delimiter="\\t")
    assert out.read_bytes() == b"a\tb\n1\t2\n"


@pytest.mark.parametrize("engine", ENGINES)
def test_rejected_rows_file(tmp_path, engine):
    src = tmp_path / "q.csv"
    put(src, 'a;b;c\n1;x;2\n2;"has;semi";3;extra\n3;y\n')
    out = tmp_path / "q.jsonl"
    r = dc.convert(src, out, engine=engine)
    # S5: the header uses the source delimiter; rows are the records as written
    assert Path(r.quarantine).read_text() == 'a;b;c\n2;"has;semi";3;extra\n3;y\n'
    # an existing rejected-rows file is not replaced without overwrite
    out.unlink()
    with pytest.raises(FileExistsError):
        dc.convert(src, out, engine=engine)
    assert not out.exists()
    # ...and a stale one goes away when a rerun rejects nothing
    clean = tmp_path / "clean.csv"
    put(clean, "a;b;c\n1;x;2\n")
    stale = tmp_path / "clean.jsonl.rejected.csv"
    put(stale, "old")
    dc.convert(clean, tmp_path / "clean.jsonl", engine=engine, overwrite=True)
    assert not stale.exists()
    # the input file is never used for rejected rows
    with pytest.raises(dc.OptionsError):
        dc.convert(src, tmp_path / "x.jsonl", engine=engine, quarantine=str(src), overwrite=True)
    assert src.read_text().startswith("a;b;c")


@pytest.mark.parametrize("engine", ENGINES)
def test_padded_rows_and_their_rejects(tmp_path, engine):
    src = tmp_path / "p.csv"
    put(src, 'a,b,c\n1,2\n3,"x,y",4,5\n6,7,8\n')
    out = tmp_path / "p.jsonl"
    r = dc.convert(src, out, engine=engine, bad_lines="pad")
    assert rows(out) == [{"a": 1, "b": 2, "c": None}, {"a": 6, "b": 7, "c": 8}]
    assert Path(r.quarantine).read_text() == 'a,b,c\n3,"x,y",4,5\n'


@pytest.mark.parametrize("engine", ENGINES)
def test_columns_in_a_new_order(tmp_path, engine):
    src = tmp_path / "o.csv"
    put(src, "a,b,c\n1,x,2024-01-01\n2,y,2024-01-02\n")
    out = tmp_path / "o.jsonl"
    r = dc.convert(src, out, engine=engine, columns=["b", "a", "c"])
    assert [n for n, _ in r.schema] == ["b", "a", "c"]
    assert rows(out)[0] == {"b": "x", "a": 1, "c": "2024-01-01"}


# --- the quote-parity fast path ----------------------------------------------------


def _exact_scan_end(buf, d):
    # the record-by-record scan alone, as it was before the fast path
    bounds, open_, irregular = _csvtok._regions(buf, d)
    for k in range(len(bounds) - 2, -1, -2):
        t = _csvtok._last_term(buf, bounds[k], bounds[k + 1], d)
        if t is not None:
            return t, open_, irregular
    return None, open_, irregular


def _exact_count(buf, d):
    bounds, _, _ = _csvtok._regions(buf, d)
    nl, cr = d.nl, d.cr
    terms = sum(buf.count(nl, s, e) + buf.count(cr, s, e) - buf.count(cr + nl, s, e) for s, e in zip(bounds[::2], bounds[1::2]))
    return terms + (1 if buf and buf[-1:] not in (nl, cr) else 0)


def _random_csv(rng, delim):
    fields = [
        "a",
        "12.5",
        "",
        " ",
        '"q"',
        '"a,b"',
        '"x\ny"',
        '"p\r\nq"',
        '"say ""hi"""',
        '""',
        '""""',
        '","',
        'é"ç',
        '5"',
        '"a"b',
        '"open',
        '""x',
        '"a\n"b',
        "é",
    ]
    recs = []
    for _ in range(rng.randint(0, 30)):
        if rng.random() < 0.1:
            recs.append(rng.choice(["\n", "\r\n", "\r"]))
            continue
        recs.append(delim.join(rng.choice(fields) for _ in range(rng.randint(1, 4))) + rng.choice(["\n", "\n", "\r\n", "\r"]))
    s = "".join(recs)
    return s[: rng.randint(0, len(s))] if s and rng.random() < 0.3 else s


def test_fast_scan_equals_the_exact_scan(monkeypatch):
    import random

    rng = random.Random(7)
    for _ in range(3000):
        delim = rng.choice([",", ";", "\t"])
        s = _random_csv(rng, delim)
        monkeypatch.setattr(_csvtok, "CUT_WINDOW", rng.choice([1, 2, 5, 16, 1 << 15]))
        for text in (True, False):
            d = _csvtok.Dialect(delim, '"', None, True, "\n", text)
            buf = s if text else s.encode()
            assert _csvtok.scan_end(buf, d) == _exact_scan_end(buf, d), (s, text)
            assert _csvtok.count_records(buf, d) == _exact_count(buf, d), (s, text)
            end = rng.randint(0, len(buf))
            assert _csvtok.scan_end(buf, d, end) == _exact_scan_end(buf[:end], d), (s, text, end)


def test_regular_runs_stop_at_records_parity_cannot_vouch_for():
    d = _csvtok.Dialect(",", '"', None, True)
    data = 'a,b\r\n1,"x,y"\n2,"p""q"\r3,""\n4,"a"b\n5,z\n6,5"7\n7,"x\ny"\n8,"open'
    starts = [s for _, s, _, _, _ in _csvtok.tokenize(data, d)]
    run = lambda pos, w=1: _csvtok.regular_run(data, d, pos, w)[0]  # noqa: E731
    assert run(0) == run(0, 4096) == starts[4]  # up to the record with text after its quote
    assert run(starts[4]) == starts[4]
    assert run(starts[5]) == starts[6]  # a quote inside an unquoted field: no telling
    assert run(starts[7]) == starts[8]  # a quoted line break is fine; a quote never closed is not
    assert _csvtok.regular_run(data, d, 0, 1, count=True) == (starts[4], 4)
    assert run(0) == _csvtok.regular_run(data.encode(), _csvtok.Dialect(",", '"', None, True, text=False), 0)[0]
    whole = 'x,y\n1,"a\r\nb"\n2,\n'
    assert _csvtok.regular_run(whole, d, 0, 2, count=True) == (len(whole), 3)
    assert _csvtok.regular_run(whole, _csvtok.Dialect(",", '"', BS, False), 0)[0] == 0  # escapes: never fast


def test_a_trailing_quote_is_not_taken_for_a_closing_one():
    # the data read so far ends on the first quote of a doubled pair, inside a field
    d = _csvtok.Dialect(",", '"', None, True, text=False)
    ch = _csvtok.Chunker(io.BytesIO(b""), 4, d, prefix=b'1,"x,y,""z""\nx,y,"')
    ch.max_record = 8
    assert ch._cut(64) is None  # more input may follow: the record is still open, read on
    ch.eof = True
    assert ch._cut(64) == 18  # at the end of the input the quote does close the field


@pytest.mark.parametrize("engine", ENGINES)
def test_regular_runs_around_irregular_records(tmp_path, engine):
    # long runs (pyarrow, the csv module) between records the tokenizer must read:
    # text after a closing quote, a quote inside a field, a malformed row, blank lines
    lines = ["id,name,note"]
    for i in range(3000):
        if i % 700 == 5:
            lines.append(f'{i},"a"b,x')
        elif i % 900 == 7:
            lines.append(f'{i},5"7,y')
        elif i % 1100 == 9:
            lines.append(f"{i},short")
        elif i % 1300 == 11:
            lines.append("")
        elif i % 2:
            lines.append(f'{i},"n, {i}","l\r\nm ""q"""')
        else:
            lines.append(f'{i},,""')
    src = tmp_path / "runs.csv"
    put(src, "\r\n".join(lines) + "\r\n")
    out = tmp_path / "runs.jsonl"
    r = dc.convert(src, out, engine=engine, types="string")
    got = rows(out)
    assert (r.rows, r.bad_rows, r.blank_lines, r.irregular_records) == (2994, 3, 3, 5)
    assert got[0] == {"id": "0", "name": None, "note": ""} and got[1] == {"id": "1", "name": "n, 1", "note": 'l\r\nm "q"'}
    assert {"id": "5", "name": "ab", "note": "x"} in got and {"id": "7", "name": '5"7', "note": "y"} in got
    assert dc.count(src, engine=engine) == r.rows + r.bad_rows  # records, malformed ones too


@pytest.mark.parametrize("engine", ENGINES)
def test_line_breaks_and_empty_fields_in_regular_runs(tmp_path, engine):
    src = tmp_path / "le.csv"
    put(src, 'a,b,c\r\n1,,""\r2,"",\n3,"x\ry",z\r\n4,,\n')
    out = tmp_path / "le.jsonl"
    dc.convert(src, out, engine=engine, types="string")
    assert rows(out) == [
        {"a": "1", "b": None, "c": ""},
        {"a": "2", "b": "", "c": None},
        {"a": "3", "b": "x\ry", "c": "z"},
        {"a": "4", "b": None, "c": None},
    ]


@pytest.mark.parametrize("engine", ENGINES)
def test_one_column_whose_values_hold_a_delimiter(tmp_path, engine):
    # S1b: a first row without the delimiter is the header of one column unless the rows
    # below show a real header (a name over typed values); MAC addresses, URLs with
    # ports, key:value pairs and a column of times with a leap second
    texts = {
        "macs": "mac\n" + "".join(f"0a:1b:2c:3d:4e:{i:02x}\n" for i in range(50)),
        "urls": "url\n" + "".join(f"https://h{i}.example.com:8080/p\n" for i in range(50)),
        "odd": "t\n" + "".join(f"2024-01-02T03:04:{60 if i == 7 else i % 60:02d}Z\n" for i in range(50)),
        "pairs": "pair\n" + "".join(f"key{i}:value{i}\n" for i in range(50)),
    }
    for name, text in texts.items():
        src = tmp_path / f"{name}.csv"
        put(src, text)
        header = text.splitlines()[0]
        d = dc.sniff(src, engine=engine)
        assert (len(d["columns"]), d["skip_rows"], d["header"], d["columns"][0]) == (1, 0, True, header), name
        out = tmp_path / f"{name}.jsonl"
        assert dc.convert(src, out, engine=engine, types="string").rows == 50
        assert rows(out)[1] == {header: text.splitlines()[2]}
    # a title line above a real header is still a preamble
    src = tmp_path / "report.csv"
    put(src, "My report\nid;name\n1;a\n2;b\n3;c\n")
    d = dc.sniff(src, engine=engine)
    assert (d["delimiter"], d["skip_rows"], d["columns"]) == (";", 1, ["id", "name"])


def test_sniffer_times_of_day():
    for v in ("9:05", "23:59:60Z", "10:00:00-03:00", "10:00:00.123456789"):
        assert _sniff._time_of_day(v), v
    for v in ("24:00", "1:2", "10:00:00+0300", "10:00:00+03", "10:00:61", "10:00:00.1234567890"):
        assert not _sniff._time_of_day(v), v


@pytest.mark.parametrize("engine", ENGINES)
def test_options_that_cannot_work_are_refused(tmp_path, engine):
    src = tmp_path / "in.csv"
    put(src, "a,b\n1,2\n")
    out = tmp_path / "out.jsonl"
    with pytest.raises(dc.OptionsError, match="unknown encoding 'bogus-enc'"):
        dc.convert(src, out, engine=engine, encoding="bogus-enc")
    with pytest.raises(dc.OptionsError, match="is also the quote or escape character; they must differ"):
        dc.convert(src, out, engine=engine, delimiter='"')
    with pytest.raises(dc.OptionsError, match="must differ"):
        dc.convert(src, out, engine=engine, delimiter=";", escape=";")
    # Python spells encodings with underscores; WHATWG labels are understood too
    for label in ("utf_8", "UTF-8", "x-mac-cyrillic", "unicode-1-1-utf-8", "latin_1"):
        assert dc.convert(src, out, engine=engine, encoding=label, overwrite=True).rows == 1, label
    # split parts are named by the pattern, never placed elsewhere
    for pattern, ext in (("../x-{n}", None), ("sub/x-{n}", None), ("x-{n}", "/.csv"), (BS.join("ab") + "-{n}", None)):
        with pytest.raises(dc.OptionsError, match="must be a file name, without folders"):
            dc.split(src, tmp_path / "parts", rows=1, pattern=pattern, extension=ext, engine=engine)
    assert not (tmp_path / "x-0.csv").exists() and not (tmp_path / "parts").exists()


@pytest.mark.parametrize("engine", ENGINES)
def test_trailing_delimiter_at_the_very_end_after_a_quoted_field(tmp_path, engine):
    # the last record has no line break: its empty last field is missing, not "" (pyarrow
    # would take it for quoted after a quoted field)
    src = tmp_path / "t.csv"
    put(src, 'a,b,c\n1,"x",\n2,"y",')
    out = tmp_path / "t.jsonl"
    dc.convert(src, out, engine=engine, types="string")
    assert rows(out) == [{"a": "1", "b": "x", "c": None}, {"a": "2", "b": "y", "c": None}]


def test_a_second_pass_reuses_the_chunks_cut(monkeypatch):
    d = _csvtok.Dialect(",", '"', None, True, text=False)
    data = b"a,b\n" + b"".join(b'%d,"x\ny"\n' % i for i in range(5000)) + b'9,"never\n10,z\n'
    known = {}
    first = [(c, ch.irregular, ch.forced, ch.stray) for ch in [_csvtok.Chunker(io.BytesIO(data), 4096, d, known=known)]
             for c in ch]
    assert b"".join(c for c, _, _, _ in first) == data and len(known) == len(first) > 2
    monkeypatch.setattr(_csvtok, "scan_end", lambda *a, **k: pytest.fail("scanned again"))
    ch = _csvtok.Chunker(io.BytesIO(data), 4096, d, known=known)
    assert [(c, ch.irregular, ch.forced, ch.stray) for c in ch] == first
