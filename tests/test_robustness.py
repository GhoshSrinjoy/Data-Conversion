"""Damaged, truncated and unusual inputs fail with a clear DataconvError, never a traceback."""

import bz2
import gzip
import io
import lzma
import random
import struct
import zipfile

import pytest

import dataconv as dc
from conftest import ENGINES, have_native
from dataconv import _compat, codec, dcz

PY_ENGINES = [e for e in ("arrow", "pure") if e in ENGINES]
DATA = b"a,b\n" + b"".join(b"%d,%d\n" % (i, i * 3) for i in range(20_000))


def _codecs():
    out = {"gz": gzip.compress, "bz2": bz2.compress, "xz": lzma.compress}
    zs = _compat.optional("zstandard")
    if zs is not None:
        out["zst"] = lambda d: zs.ZstdCompressor().compress(d)
    lz4f = _compat.optional("lz4.frame")
    if lz4f is not None:
        out["lz4"] = lz4f.compress
    return out


@pytest.mark.parametrize("engine", PY_ENGINES)
@pytest.mark.parametrize("ext", sorted(_codecs()))
def test_every_member_or_frame_is_read(tmp_path, engine, ext):
    pack = _codecs()[ext]
    src = tmp_path / f"m.csv.{ext}"
    src.write_bytes(pack(DATA) + pack(DATA[4:]))  # two members / streams / frames
    r = dc.convert(src, tmp_path / "m.jsonl", engine=engine)
    assert r.rows == 40_000


@pytest.mark.parametrize("engine", PY_ENGINES)
@pytest.mark.parametrize("ext", sorted(_codecs()))
def test_truncated_compressed_input(tmp_path, engine, ext):
    packed = _codecs()[ext](DATA)
    src = tmp_path / f"t.csv.{ext}"
    src.write_bytes(packed[: len(packed) // 2])
    with pytest.raises(dc.IntegrityError, match="truncated or corrupt"):
        dc.convert(src, tmp_path / "t.jsonl", engine=engine)
    assert not (tmp_path / "t.jsonl").exists()


@pytest.mark.parametrize("engine", PY_ENGINES)
def test_corrupt_zip_and_brotli(tmp_path, engine):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("x.csv", DATA)
    flipped = bytearray(buf.getvalue())
    flipped[100] ^= 0xFF
    src = tmp_path / "x.csv.zip"
    src.write_bytes(bytes(flipped))
    with pytest.raises(dc.IntegrityError):
        dc.convert(src, tmp_path / "x.jsonl", engine=engine)
    (tmp_path / "half.csv.zip").write_bytes(buf.getvalue()[:200])
    with pytest.raises(dc.IntegrityError):
        dc.convert(tmp_path / "half.csv.zip", tmp_path / "h.jsonl", engine=engine)
    br = _compat.optional("brotli")
    if br is not None:
        packed = br.compress(DATA)
        (tmp_path / "b.csv.br").write_bytes(packed[: len(packed) // 2])
        with pytest.raises(dc.IntegrityError):
            dc.convert(tmp_path / "b.csv.br", tmp_path / "b.jsonl", engine=engine)


def _dcz_file(tmp_path, no_native=True):
    src = tmp_path / "c.csv"
    src.write_bytes(DATA)
    dst = tmp_path / "c.dcz"
    dc.compress(src, dst, engine="pure", block_size=16384)
    return dst


def test_bit_flipped_dcz_fails_cleanly(tmp_path):
    raw = _dcz_file(tmp_path).read_bytes()
    rng = random.Random(3)
    bad = tmp_path / "bad.dcz"
    for _ in range(250):
        b = bytearray(raw)
        b[rng.randrange(len(b))] ^= 1 << rng.randrange(8)
        bad.write_bytes(bytes(b))
        try:
            out = b"".join(dcz.iter_decoded(bad))
        except dc.DataconvError:
            continue
        assert out == DATA  # a flip that changes nothing that matters (a reserved byte)


def test_block_index_must_tile_the_file(tmp_path):
    path = _dcz_file(tmp_path)
    raw = bytearray(path.read_bytes())
    footer = dcz.read_footer(path)
    assert len(footer["blocks"]) > 1
    # a block header that disagrees with its (checksummed) index entry
    first = footer["blocks"][0]["offset"]
    b = bytearray(raw)
    struct.pack_into("<Q", b, first + 4, footer["blocks"][0]["raw_len"] + 1)
    path.write_bytes(bytes(b))
    with pytest.raises(dc.IntegrityError, match="does not match"):
        b"".join(dcz.iter_decoded(path))


def test_nested_rle_and_oversized_counts_are_rejected():
    inner = bytearray()
    dcz.put_varint(inner, 4)
    inner.append(2)  # an RLE array ...
    dcz.put_varint(inner, 1)
    inner += dcz.encode_ints([7], 2)  # ... whose values are RLE themselves
    inner += dcz.encode_ints([4], 0)
    with pytest.raises(dc.IntegrityError):
        dcz.decode_ints(dcz.Reader(bytes(inner)))
    with pytest.raises(dc.IntegrityError):
        dcz.decode_ints(dcz.Reader(dcz.encode_ints([1] * 10)), limit=5)


def test_decompression_is_bounded():
    data = b"x" * 100_000
    for c in (codec.ZLIB, codec.LZMA, codec.ZSTD, codec.LZ4, codec.BROTLI):
        if not codec.available(c):
            continue
        packed = codec.compress(c, data, 1)
        assert codec.decompress(c, packed, len(data)) == data
        with pytest.raises(dc.IntegrityError):
            codec.decompress(c, packed, 10)
    with pytest.raises(dc.IntegrityError):
        codec.decompress(2, b"", 0)  # a codec id no writer uses is corruption


@pytest.mark.parametrize("engine", PY_ENGINES)
def test_fallback_engines_read_dcz(tmp_path, engine):
    path = _dcz_file(tmp_path)
    assert dc.count(path, engine=engine) == 20_000
    assert dc.head(path, 2, engine=engine)["rows"] == [["0", "0"], ["1", "3"]]
    assert dc.info(path, engine=engine)["original_size"] == len(DATA)
    r = dc.convert(path, tmp_path / "c.jsonl", engine=engine)
    assert r.rows == 20_000


def test_compress_keeps_the_files_own_bytes(tmp_path, no_native):
    src = tmp_path / "x.csv.gz"
    src.write_bytes(gzip.compress(DATA))
    dc.compress(src)
    back = tmp_path / "back.bin"
    dc.decompress(tmp_path / "x.csv.gz.dcz", back)
    assert back.read_bytes() == src.read_bytes()
    # converting a .csv.gz to DCZ still stores the CSV
    dc.convert(src, tmp_path / "x.dcz")
    assert b"".join(dcz.iter_decoded(tmp_path / "x.dcz")) == DATA


@pytest.mark.skipif(not have_native(), reason="native engine not installed")
def test_native_compress_keeps_the_files_own_bytes(tmp_path):
    src = tmp_path / "x.csv.gz"
    src.write_bytes(gzip.compress(DATA))
    dc.compress(src, engine="native")
    dc.decompress(tmp_path / "x.csv.gz.dcz", tmp_path / "back.bin", engine="native")
    assert (tmp_path / "back.bin").read_bytes() == src.read_bytes()


def test_memory_budget_reaches_compress_and_decompress(tmp_path, monkeypatch):
    from dataconv import _native
    from dataconv.cli import main
    from dataconv.engines import native

    seen = []

    def fake(name, *args, **kwargs):
        seen.append((name, kwargs))
        raise dc.DataError("stop here")

    monkeypatch.setattr(_native, "available", lambda: True)
    monkeypatch.setattr(native._native, "call_json", fake)
    src = tmp_path / "a.csv"
    src.write_bytes(DATA)
    for kw, want in (({}, {}), ({"memory": "64MB"}, {"memory": 64 << 20}), ({"memory": 1 << 30}, {"memory": 1 << 30})):
        with pytest.raises(dc.DataError):
            dc.compress(src, tmp_path / "a.dcz", **kw)
        with pytest.raises(dc.DataError):
            dc.decompress(src, tmp_path / "back", **kw)
        # only a budget that is set is passed: older native modules take no such keyword
        assert seen[-2:] == [("compress", want), ("decompress", want)]
    monkeypatch.undo()
    # the Python fallback plans no buffering of its own; the command line passes --memory
    assert main(["compress", str(src), str(tmp_path / "b.dcz"), "--memory", "64MB", "--engine", "pure", "--quiet"]) == 0
    assert (
        main(["decompress", str(tmp_path / "b.dcz"), str(tmp_path / "b.csv"), "--memory", "64", "--engine", "pure", "--quiet"])
        == 0
    )
    assert (tmp_path / "b.csv").read_bytes() == DATA


@pytest.mark.parametrize("engine", ENGINES)
def test_a_50mb_quote_that_never_closes(tmp_path, engine):
    import time

    # the stray rule: the record ends with the line that holds the quote (no scan of the
    # rest of the input as one field, no backtracking)
    src = tmp_path / "unclosed.csv"
    src.write_bytes(b'id,v\n1,ok\n2,"' + b"x" * (50 << 20) + b"\n3,y\n")
    out = tmp_path / "unclosed.csv.out.csv"
    started = time.perf_counter()
    r = dc.convert(src, out, engine=engine)
    assert time.perf_counter() - started < 60
    assert (r.rows, r.bad_rows) == (2, 1) and out.read_bytes() == b"id,v\n1,ok\n3,y\n"
    assert dc.count(src, engine=engine) == 3
    # doubled quotes all along a field that never closes
    src.write_bytes(b'id,v\n1,ok\n2,"' + b'ab""cd' * (1 << 20) + b"\n3,y\n")
    started = time.perf_counter()
    r = dc.convert(src, out, engine=engine, overwrite=True)
    assert time.perf_counter() - started < 60
    assert (r.rows, r.bad_rows) == (2, 1)


@pytest.mark.parametrize("engine", PY_ENGINES)
def test_a_quote_read_as_a_stray_by_its_size_is_reported(tmp_path, engine, monkeypatch):
    from dataconv import _csvtok

    # MAX_QUOTED (1 GiB, as in the native engine) cut down so that it is reached
    monkeypatch.setattr(_csvtok, "MAX_QUOTED", 1 << 16)
    head = b'a,b\n1,"never closed\n'
    src = tmp_path / "long.csv"
    src.write_bytes(head + b"".join(b"%d,x\n" % i for i in range(700_000)))  # past one arrow chunk
    r = dc.convert(src, tmp_path / "long.jsonl", engine=engine)
    assert (r.rows, r.bad_rows) == (700_000, 1)
    notes = [w for w in r.warnings if "read as a stray quote" in w]
    assert notes == [_csvtok.forced_warning(len(head))]
    assert notes[0].startswith(f"a quote before byte {len(head)} was still open")
    monkeypatch.undo()
    assert _csvtok.forced_warning(7) == (
        "a quote before byte 7 was still open 1024 MiB later and was read as a stray quote; "
        "if the file really holds a quoted field that large, the rows after it need checking"
    )
    # at the end of the input a quote that never closes is a stray without a note
    small = tmp_path / "small.csv"
    small.write_bytes(head + b"2,y\n")
    r = dc.convert(small, tmp_path / "small.jsonl", engine=engine)
    assert r.bad_rows == 1 and not [w for w in r.warnings if "read as a stray quote" in w]


def test_directories_are_named(tmp_path):
    for fn in (dc.count, dc.info, dc.head, dc.sniff, dc.compress, dc.decompress):
        with pytest.raises(dc.FileError, match="is a directory, not a file"):
            fn(tmp_path)


def test_engine_messages(tmp_path):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("x.csv", DATA)
    src = tmp_path / "x.csv.zip"
    src.write_bytes(buf.getvalue())
    if have_native():
        with pytest.raises(dc.OptionsError, match="cannot read zip-compressed input"):
            dc.convert(src, tmp_path / "x.parquet", engine="native")
        with pytest.raises(dc.OptionsError, match="cannot read zip-compressed input"):
            dc.count(src, engine="native")
    else:
        with pytest.raises(dc.MissingDependencyError, match="not installed"):
            dc.count(src, engine="native")


@pytest.mark.parametrize("engine", PY_ENGINES)
def test_internal_restarts_never_escape(tmp_path, engine):
    # invalid UTF-8 past the sniffer's 2 MB sample but inside the inference sample
    body = b"city,n\n" + b"".join(b"Paris-%020d,%d\n" % (i, i) for i in range(80_000)) + b"Z\xfcrich,1\n"
    src = tmp_path / "late.csv"
    src.write_bytes(body + b"".join(b"Rome,%d\n" % i for i in range(10)))
    r = dc.convert(src, tmp_path / "late.jsonl", engine=engine)
    assert r.rows == 80_011 and any("windows-1252" in w for w in r.warnings)
    # values no typed column could hold force text, in head() too
    dates = tmp_path / "d.csv"
    dates.write_bytes(b"d\n2024-01-01\n0000-01-01\n")
    assert dc.head(dates, 5, engine=engine)["rows"] == [["2024-01-01"], ["0000-01-01"]]


TYPED_ENGINES = [e for e in ("native", "arrow") if e in ENGINES]
needs_arrow = pytest.mark.skipif("arrow" not in ENGINES, reason="pyarrow not installed")


def _ipc(path, table):
    pa = pytest.importorskip("pyarrow")
    with pa.ipc.new_file(pa.OSFile(str(path), "wb"), table.schema) as w:
        w.write_table(table)
    return path


@needs_arrow
@pytest.mark.parametrize("engine", TYPED_ENGINES)
def test_list_views_keep_null_items(tmp_path, engine):
    import pyarrow as pa

    if not hasattr(pa, "list_view"):
        pytest.skip("pyarrow without list views")
    src = _ipc(tmp_path / "lv.arrow", pa.table({"lv": pa.array([[1, 2], [], None, [None]], pa.list_view(pa.int64()))}))
    out = tmp_path / "lv.jsonl"
    dc.convert(src, out, engine=engine)
    assert out.read_text().splitlines() == ['{"lv":[1,2]}', '{"lv":[]}', '{"lv":null}', '{"lv":[null]}']


@needs_arrow
@pytest.mark.parametrize("engine", TYPED_ENGINES)
def test_parquet_dates_keep_what_they_hold(tmp_path, engine):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    # a date64 with a time of day cannot become a Parquet date: refused, not cut
    src = _ipc(tmp_path / "d.arrow", pa.table({"d": pa.array([86_400_000 + 1234, None], pa.int64()).cast(pa.date64())}))
    with pytest.raises(dc.SchemaError, match="time of day"):
        dc.convert(src, tmp_path / "d.parquet", engine=engine)
    assert not list(tmp_path.glob(".*"))
    # it still converts to text, keeping the time
    dc.convert(src, tmp_path / "d.csv", engine=engine)
    assert (tmp_path / "d.csv").read_text().splitlines()[1] == "1970-01-02T00:00:01.234"
    # a null stored among a dictionary's values stays a null
    d = pc.dictionary_encode(pa.array(["a", None, "a", "b"]), null_encoding="encode")
    src = _ipc(tmp_path / "dn.arrow", pa.table({"d": d}))
    dc.convert(src, tmp_path / "dn.parquet", engine=engine)
    assert pq.read_table(tmp_path / "dn.parquet").column(0).to_pylist() == ["a", None, "a", "b"]


@needs_arrow
@pytest.mark.parametrize("engine", TYPED_ENGINES)
def test_a_type_parquet_cannot_hold_leaves_nothing_behind(tmp_path, engine):
    import pyarrow as pa

    src = _ipc(tmp_path / "se.arrow", pa.table({"se": pa.array([{}, None], pa.struct([]))}))
    with pytest.raises(dc.DataconvError):
        dc.convert(src, tmp_path / "se.parquet", engine=engine)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["se.arrow"]


@needs_arrow
@pytest.mark.parametrize("engine", TYPED_ENGINES)
def test_corrupt_arrow_files_fail_cleanly(tmp_path, engine):
    import pyarrow as pa

    good = _ipc(tmp_path / "f.arrow", pa.table({"s": [f"name-{i}" for i in range(50)]})).read_bytes()
    for i in range(0, len(good), 3):
        bad = bytearray(good)
        bad[i] ^= 0x80
        src = tmp_path / "flip.arrow"
        src.write_bytes(bytes(bad))
        try:
            dc.convert(src, tmp_path / "flip.csv", engine=engine, overwrite=True)
        except dc.DataconvError:
            pass
    assert not list(tmp_path.glob(".*"))


@pytest.mark.parametrize("engine", ENGINES)
def test_head_of_a_value_no_type_holds(tmp_path, engine):
    src = tmp_path / "s.jsonl"
    src.write_bytes(b'{"s": "\\ud83d\\ude00"}\n{"s": "\\ud800"}\n')  # the second is a lone surrogate
    with pytest.raises(dc.DataError, match="does not fit"):
        dc.head(src, 5, engine=engine)
