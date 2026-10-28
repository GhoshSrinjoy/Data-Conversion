import random
from pathlib import Path

import pytest

from vegg import _compat, _native


def have_native() -> bool:
    return _native.available()


def have_arrow() -> bool:
    return _compat.have("pyarrow")


needs_native = pytest.mark.skipif(not have_native(), reason="the native engine is not installed")
needs_arrow = pytest.mark.skipif(not have_arrow(), reason="pyarrow not installed")

ENGINES = [e for e, ok in (("native", have_native()), ("arrow", have_arrow()), ("pure", True)) if ok]


@pytest.fixture
def no_native(monkeypatch):
    """Run the test as if the native engine were not installed."""
    monkeypatch.setattr(_native, "module", lambda: None)
    yield


TRICKY_HEADER = "id,zip,big,price,precise,when,day,ok,name,note"


def tricky_csv(rows: int = 2000, bad: bool = True) -> str:
    """CSV that trips naive converters: leading zeros, huge ints, high-precision
    decimals, offsets, quoted delimiters/newlines, NA markers, empty vs missing."""
    out = [TRICKY_HEADER]
    for i in range(rows):
        zip_code = f"{(i * 7919) % 100000:05d}"
        big = "18446744073709551615" if i == 17 else str(i * 1_000_003)
        price = "NA" if i % 97 == 0 else f"{i % 1000}.{i % 100:02d}"
        precise = "1.00000000000000000001" if i == 3 else f"{i}.5"
        when = f"2024-01-02T03:{i % 60:02d}:{(i * 7) % 60:02d}.{i % 1000:03d}+05:30"
        day = f"2023-{i % 12 + 1:02d}-{i % 28 + 1:02d}"
        ok = "true" if i % 2 == 0 else "False"
        name = ['"Smith, J"', "NA", '"line\nbreak"', f"user{i}"][i % 4]
        note = "" if i % 3 == 0 else '""'
        out.append(f"{i},{zip_code},{big},{price},{precise},{when},{day},{ok},{name},{note}")
        if bad and i == rows // 2:
            out.append("this,row,is,broken")
            out.append("")
    return "\n".join(out) + "\n"


@pytest.fixture
def tricky(tmp_path: Path) -> Path:
    p = tmp_path / "tricky.csv"
    p.write_bytes(tricky_csv().encode("utf-8"))
    return p


def random_csv(rng: random.Random, rows: int) -> bytes:
    """Random delimited text with every quoting / terminator edge case."""
    pieces = []
    cols = rng.randint(1, 6)
    terms = [b"\n", b"\r\n", b"\r"] if rng.random() < 0.3 else [b"\n"]
    for _ in range(rows):
        fields = []
        for _ in range(cols):
            kind = rng.random()
            if kind < 0.25:
                fields.append(str(rng.randint(-10**6, 10**6)).encode())
            elif kind < 0.4:
                fields.append(f"{rng.uniform(-1000, 1000):.{rng.randint(0, 4)}f}".encode())
            elif kind < 0.5:
                fields.append(b"")
            elif kind < 0.6:
                fields.append(b'"' + rng.choice([b"a,b", b'say ""hi""', b"multi\nline", b"", b"x"]) + b'"')
            elif kind < 0.65:
                fields.append(rng.choice([b"007", b"-0", b"1e5", b"+3", b"0x1F", "é€".encode(), b"\xff\xfe"]))
            elif kind < 0.7:
                fields.append(f"2024-01-{rng.randint(1, 28):02d} 10:{rng.randint(0, 59):02d}:00".encode())
            else:
                fields.append(rng.choice([b"alpha", b"beta", b"gamma", b"delta"]) + str(rng.randint(0, 50)).encode())
        if rng.random() < 0.02:
            fields = fields[: rng.randint(0, len(fields))]  # ragged
        if rng.random() < 0.01:
            fields.append(b'"unbalanced')
        pieces.append(b",".join(fields) + rng.choice(terms))
    data = b"".join(pieces)
    if rng.random() < 0.3:
        data = data.rstrip(b"\r\n")  # no final terminator
    return data
