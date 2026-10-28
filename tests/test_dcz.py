import random

import pytest

import vegg
from conftest import needs_native, random_csv
from vegg import dcz


@pytest.mark.parametrize("seed", range(40))
def test_python_block_codec_is_byte_exact(seed):
    rng = random.Random(seed)
    data = random_csv(rng, rng.randint(1, 400))
    for profile in ("balanced", "max"):
        header, payload, kind, _ = dcz.encode_block(data, (",", '"'), True, profile)
        assert dcz.decode_block(kind, header[1], payload, len(data), dcz._crc(data)) == data


@pytest.mark.parametrize("profile", ["fast", "balanced", "max"])
def test_pure_file_roundtrip(tmp_path, profile, no_native):
    rng = random.Random(7)
    data = b"".join(random_csv(rng, 300) for _ in range(20))
    src = tmp_path / "in.csv"
    src.write_bytes(data)
    dst = tmp_path / "in.csv.dcz"
    vegg.compress(src, dst, profile=profile, block_size=8192)
    assert dcz.read_footer(dst)["original_size"] == len(data)
    back = tmp_path / "back.csv"
    vegg.decompress(dst, back)
    assert back.read_bytes() == data


@needs_native
@pytest.mark.parametrize("seed", range(10))
def test_native_and_python_read_each_other(tmp_path, seed, monkeypatch):
    rng = random.Random(100 + seed)
    data = b"".join(random_csv(rng, 200) for _ in range(10))
    src = tmp_path / "x.csv"
    src.write_bytes(data)
    native = tmp_path / "native.dcz"
    vegg.compress(src, native, profile=rng.choice(["balanced", "max"]), block_size=16384)
    # decode the native file with the pure-Python decoder
    assert b"".join(dcz.iter_decoded(native)) == data
    # and a pure-Python file with the native engine
    from vegg import _native

    real = _native.module
    monkeypatch.setattr(_native, "module", lambda: None)
    pure = tmp_path / "pure.dcz"
    vegg.compress(src, pure, profile="balanced", block_size=16384)
    monkeypatch.setattr(_native, "module", real)
    out = tmp_path / "pure_by_native.csv"
    vegg.decompress(pure, out)
    assert out.read_bytes() == data


def test_binary_and_empty_files(tmp_path, no_native):
    for name, data in (("blob.bin", bytes(range(256)) * 300), ("empty.csv", b""), ("one.csv", b"x")):
        src = tmp_path / name
        src.write_bytes(data)
        dst = tmp_path / (name + ".dcz")
        vegg.compress(src, dst)
        out = tmp_path / (name + ".out")
        vegg.decompress(dst, out)
        assert out.read_bytes() == data


def test_corruption_is_detected(tmp_path, no_native):
    src = tmp_path / "c.csv"
    src.write_bytes(b"a,b\n" + b"".join(b"%d,%d\n" % (i, i * i) for i in range(5000)))
    dst = tmp_path / "c.dcz"
    vegg.compress(src, dst)
    raw = bytearray(dst.read_bytes())
    raw[60] ^= 0xFF
    bad = tmp_path / "bad.dcz"
    bad.write_bytes(bytes(raw))
    with pytest.raises(vegg.VeggError):
        vegg.decompress(bad, tmp_path / "bad.out")
    (tmp_path / "trunc.dcz").write_bytes(bytes(raw[: len(raw) // 2]))
    with pytest.raises(vegg.VeggError):
        vegg.decompress(tmp_path / "trunc.dcz", tmp_path / "trunc.out")


def test_columnar_beats_plain_compression(tmp_path):
    import zlib

    lines = ["ts,sensor,value"] + [f"{1700000000 + i * 10},S{i % 4},{20 + i % 7}.{i % 10}" for i in range(50_000)]
    data = ("\n".join(lines) + "\n").encode()
    src = tmp_path / "s.csv"
    src.write_bytes(data)
    dst = tmp_path / "s.dcz"
    vegg.compress(src, dst, profile="balanced")
    assert dst.stat().st_size * 3 < len(zlib.compress(data, 9))


def test_int_codec_extremes():
    for values in ([], [5], [-(1 << 63), (1 << 63) - 1, 0, -1], list(range(0, 10**6, 3)), [7] * 1000):
        enc = dcz.encode_ints(values)
        assert dcz.decode_ints(dcz.Reader(enc)) == values
    assert len(dcz.encode_ints(list(range(10_000)))) < 32


def test_block_cuts_follow_the_quote_rules():
    # a literal quote inside a field does not hide the records after it
    data = b'id,size\n1,55" screen\n2,x\n'
    assert dcz._record_boundary(data, '"') == len(data)
    # an escaped quote does not close a field
    esc = b'1,"a\\"b\nc"\n2,x\n'
    assert dcz._record_boundary(esc[: esc.index(b"c")], '"', ",", "\\") == 0
    assert dcz._record_boundary(esc, '"', ",", "\\") == len(esc)


def test_reader_is_a_binary_stream(tmp_path, no_native):
    import io

    src = tmp_path / "r.csv"
    src.write_bytes(b"a,b\n" + b"".join(b"%d,x\n" % i for i in range(1000)))
    dst = tmp_path / "r.dcz"
    vegg.compress(src, dst)
    with io.TextIOWrapper(io.BufferedReader(dcz.DczReader(dst)), encoding="utf-8", newline="") as f:
        assert f.read() == src.read_bytes().decode()
