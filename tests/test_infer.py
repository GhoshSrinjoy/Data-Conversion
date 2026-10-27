import pytest

from dataconv import infer
from dataconv.infer import ColumnStats, LogicalType, parse_float, parse_type


def decide(values, mode="infer"):
    s = ColumnStats()
    s.observe_many(values, mode)
    return s.decide().label


@pytest.mark.parametrize(
    "values, expected",
    [
        (["1", "-2", "30"], "int64"),
        (["007", "123"], "string"),
        (["+4915112345678"], "string"),
        (["0x10"], "string"),
        (["18446744073709551615", "1"], "decimal(20,0)"),
        (["9223372036854775807", "-9223372036854775808"], "int64"),
        (["1", "", "NA", "3"], "int64"),
        (["0.1", "2.50", "1e-5", "-3"], "float64"),
        (["0.30000000000000004", "0.1"], "float64"),
        (["1.00000000000000000001"], "decimal(21,20)"),
        (["1.5", "nan", "inf"], "float64"),
        (["00.5"], "string"),
        (["1e400"], "string"),
        (["2024-01-02", "1999-12-31"], "date"),
        (["2024-02-30"], "string"),
        (["2024-01-02 03:04:05", "2024-01-02T03:04:05.5"], "timestamp[ms]"),
        (["2024-01-02T03:04:05Z", "2024-01-02 03:04:05.123456+00:00"], "timestamp[us,UTC]"),
        (["2024-01-02T03:04:05+05:30"], "timestamp[s,+05:30]"),
        (["2024-01-02T03:04:05+05:30", "2024-01-02T03:04:05Z"], "string"),
        (["-0", "1"], "float64"),  # -0 keeps its sign only as a float
        (["1.2345e-320"], "string"),
        (["true", "False", "TRUE"], "bool"),
        (["1", "0"], "int64"),
        (["", "NA"], "string"),
        (["NA", "Namibia"], "string"),
    ],
)
def test_rules(values, expected):
    assert decide(values) == expected


def test_modes():
    assert decide(["007"], "loose") == "int64"
    assert decide(["1", "2"], "string") == "string"


def test_faithful_17_digit_floats():
    for x in (0.1, 1 / 3, 2**0.5, 1e-300, 123456.789):
        assert parse_float(f"{x:.16e}").lossless
    assert not parse_float("0.50000000000000006").lossless


def test_explain():
    s = ColumnStats()
    s.observe_many(["02134", "10001"])
    assert "leading zeros" in s.explain("zip")


def test_parse_type():
    assert parse_type("decimal(10, 2)") == LogicalType("decimal", 10, 2)
    assert parse_type("timestamp[ms,utc]") == LogicalType("timestamp", unit="ms", tz="UTC")
    with pytest.raises(ValueError):
        parse_type("decimal(50,1)")


def test_converters_are_lossless():
    conv = infer.converter(LogicalType("decimal", 20, 0))
    assert str(conv("18446744073709551615")) == "18446744073709551615"
    with pytest.raises(ValueError):
        infer.converter(LogicalType("int64"))("007")
    assert infer.converter(LogicalType("int64"))("NA") is None
    assert infer.converter(LogicalType("string"))("NA") == "NA"


def test_days_from_civil():
    assert infer.days_from_civil(1970, 1, 1) == 0
    assert infer.days_from_civil(2000, 3, 1) == 11017
    assert infer.parse_date("1969-12-31") == -1


@pytest.mark.skipif(not __import__("dataconv")._native.available(), reason="native engine not installed")
def test_python_rules_match_native(tmp_path):
    """Differential test: both engines must assign the same type to every column."""
    import dataconv as dc

    columns = {
        "ints": ["1", "-2", "", "NA"],
        "zeros": ["007", "12", "3", "4"],
        "big": ["18446744073709551615", "1", "2", "3"],
        "floats": ["0.1", "2.50", "1e-5", "NaN"],
        "precise": ["1.00000000000000000001", "2", "3", "4"],
        "dates": ["2024-01-02", "1999-12-31", "", "2000-02-29"],
        "ts": ["2024-01-02T03:04:05Z", "2024-01-02 03:04:05.123456+00:00", "", "2024-01-02T03:04:05Z"],
        "ts_mixed": ["2024-01-02T03:04:05+05:30", "2024-01-02T03:04:05Z", "", "2024-01-02T03:04:05Z"],
        "bools": ["true", "False", "TRUE", ""],
        "text": ["NA", "Namibia", "x", "y"],
    }
    names = list(columns)
    lines = [",".join(names)] + [",".join(columns[n][i] for n in names) for i in range(4)]
    src = tmp_path / "cols.csv"
    src.write_text("\n".join(lines) + "\n")
    native = dict(dc.convert(src, tmp_path / "n.jsonl", engine="native").schema)
    for name in names:
        assert native[name] == decide(columns[name]), name
