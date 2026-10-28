import array
import math
import struct

import pytest

import dataconv as dc
from conftest import have_native

MODES = ["native", "pure"] if have_native() else ["pure"]


@pytest.fixture(params=MODES)
def mode(request, monkeypatch):
    if request.param == "pure":
        from dataconv import _native

        monkeypatch.setattr(_native, "module", lambda: None)
    return request.param


def bits(values):
    return [struct.pack("<d", v) for v in values]


@pytest.mark.parametrize("profile", ["fast", "balanced", "max"])
def test_floats_bit_exact(mode, profile):
    values = [i / 100 for i in range(20000)] + [math.nan, -0.0, 0.0, math.inf, -math.inf, 5e-324, 1.7976931348623157e308, 0.1 + 0.2]
    blob = dc.pack(values, profile=profile)
    assert bits(dc.unpack(blob)) == bits(values)
    assert len(blob) < len(values) * 2  # decimal-looking floats pack to integer mantissas


def test_ints_and_nones(mode):
    values = list(range(0, 1_000_000, 7))
    blob = dc.pack(values)
    assert dc.unpack(blob) == values
    assert len(blob) < 200
    assert dc.unpack(dc.pack([1, None, 2**70, -5])) == [1, None, 2**70, -5]
    assert dc.unpack(dc.pack([True, False, None])) == [True, False, None]


def test_strings_and_containers(mode):
    ids = [f"INV-{i:06d}" for i in range(5000)]
    blob = dc.pack(ids)
    assert dc.unpack(blob) == ids and len(blob) < 400
    assert dc.unpack(dc.pack(["", "ünïcödé", "a,b\n"])) == ["", "ünïcödé", "a,b\n"]
    assert dc.unpack(dc.pack((1, 2, 3))) == (1, 2, 3)
    a = array.array("i", [1, -2, 3])
    assert dc.unpack(dc.pack(a)) == a
    mixed = [1, "a", {"k": [1.5, None]}, None]
    assert dc.unpack(dc.pack(mixed)) == mixed
    assert dc.unpack(dc.pack([])) == []


def test_numpy(mode):
    from dataconv import _compat

    np = _compat.optional("numpy")
    if np is None:
        pytest.skip("numpy not available (or disabled)")
    for arr in (
        np.arange(12, dtype=np.float32).reshape(3, 4) / 3,
        np.arange(-5, 5, dtype=np.int8),
        np.array([2**64 - 1, 0], dtype=np.uint64),
        np.array([True, False]),
        np.array(["2024-01-01", "NaT", "1999-12-31"], dtype="datetime64[s]"),
    ):
        back = dc.unpack(dc.pack(arr))
        assert back.dtype == arr.dtype and back.shape == arr.shape
        assert np.array_equal(back.view(np.uint8), np.ascontiguousarray(arr).view(np.uint8))


@pytest.mark.skipif(not have_native(), reason="needs both implementations")
def test_cross_implementation(monkeypatch):
    from dataconv import _native

    values = [i * 0.25 for i in range(1000)] + [math.nan]
    native_blob = dc.pack(values)
    real = _native.module
    monkeypatch.setattr(_native, "module", lambda: None)
    assert bits(dc.unpack(native_blob)) == bits(values)
    pure_blob = dc.pack(values)
    monkeypatch.setattr(_native, "module", real)
    assert bits(dc.unpack(pure_blob)) == bits(values)


def test_rejects_garbage(mode):
    with pytest.raises(dc.DataconvError):
        dc.unpack(b"not packed data at all")


def test_containers_are_restored_exactly(mode):
    mixed = (1, "a", None, 2.5)
    back = dc.unpack(dc.pack(mixed))
    assert type(back) is tuple and back == mixed
    assert dc.unpack(dc.pack([b"\xff\x00", None, b""])) == [b"\xff\x00", None, b""]
    values = [1 + 2j, -0.0 - 1j, complex(math.inf, math.nan)]
    back = dc.unpack(dc.pack(values))
    assert bits([c.real for c in back]) == bits([c.real for c in values])
    assert bits([c.imag for c in back]) == bits([c.imag for c in values])
    assert dc.unpack(dc.pack([1j, None])) == [1j, None]


def test_numpy_scalars_and_dtypes(mode):
    from dataconv import _compat

    np = _compat.optional("numpy")
    if np is None:
        pytest.skip("numpy not available (or disabled)")
    assert dc.unpack(dc.pack([np.int64(1), np.float32(1.5), None])) == [1, 1.5, None]
    obj = np.array([[1, "a"], [None, [2, 3]]], dtype=object)
    back = dc.unpack(dc.pack(obj))
    assert isinstance(back, np.ndarray) and back.shape == (2, 2) and back.dtype == object
    assert back.tolist() == obj.tolist()
    for arr in (
        np.array([1 + 2j, 3j], dtype=np.complex64),
        np.array([[1 + 2j], [-0.0j]], dtype=np.complex128),
        np.array([b"a", b"bc\xff"]),
        np.array(["x", "é"]),
    ):
        back = dc.unpack(dc.pack(arr))
        assert back.dtype == arr.dtype and back.shape == arr.shape
        assert back.tobytes() == arr.tobytes()


def test_values_json_would_change_are_refused(mode):
    for values in ([(1, 2), (3, 4)], [{1: "a"}], [{"a": {1, 2}}], [__import__("decimal").Decimal("1.5"), 1]):
        with pytest.raises(dc.OptionsError):
            dc.pack(values)
    for values in ("abc", b"abc", {"a": 1}, 5):
        with pytest.raises(dc.OptionsError):
            dc.pack(values)
