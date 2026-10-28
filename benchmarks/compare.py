"""Compare vegg with DuckDB, Polars, pyarrow and pandas on CSV -> Parquet.

Every tool converts the same generated export with its default settings. The script
reports the best time of several runs (inside one process, so start-up is not
counted), the size of the output, how many values read back differently from the
CSV text, and what each tool does with a file that has one malformed row.

    pip install vegg duckdb polars pyarrow pandas
    python benchmarks/compare.py                  # 2 million rows, best of 3
    python benchmarks/compare.py --rows 500000 --runs 5 --dir /tmp/vegg-bench

The generated files and the outputs go to --dir (default: a "bench-data" folder
next to this script).
"""

from __future__ import annotations

import argparse
import csv
import gc
import time
from decimal import Decimal
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

import vegg

COUNTRIES = ["DE", "FR", "IN", "US", "NA", "BR", "JP"]  # NA is Namibia
CHECKED = {
    "zip": "zip codes (leading zeros)",
    "country": "country codes (`NA` is Namibia)",
    "phone": "phone numbers (`+49...`)",
    "account": "20-digit account numbers",
}


def make_export(path: Path, rows: int) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["id", "created_at", "user", "price", "zip", "country", "phone", "account"])
        for i in range(rows):
            w.writerow([
                i,
                f"2024-{i % 12 + 1:02d}-{i % 28 + 1:02d}T{i % 24:02d}:{i % 60:02d}:{i * 7 % 60:02d}+05:30",
                f"user_{i % 50_000}",
                "NA" if i % 97 == 0 else f"{i * 37 % 100_000 / 100:.2f}",
                f"{i * 7919 % 100_000:05d}",
                COUNTRIES[i % 7],
                f"+49151{i * 1_234_567 % 100_000_000:08d}",
                12_345_678_901_234_567_890 + i,
            ])


def make_messy(path: Path, rows: int) -> None:
    """One row with a field too many, and one quote that is never closed."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        f.write("id,name,city,amount\n")
        for i in range(rows):
            if i == rows * 3 // 10:
                f.write(f"{i},user{i},Paris,1.50,EXTRA\n")
            elif i == rows * 6 // 10:
                f.write(f'{i},"user{i},Pune,2.25\n')
            else:
                f.write(f"{i},user{i},Rome,{i % 1000}.25\n")


TOOLS = {
    "vegg": lambda src, dst: vegg.convert(src, dst, overwrite=True),
    "DuckDB": lambda src, dst: duckdb.execute(
        f"COPY (SELECT * FROM read_csv('{src.as_posix()}')) TO '{dst.as_posix()}' (FORMAT parquet)"),
    "Polars": lambda src, dst: pl.scan_csv(src).sink_parquet(dst),
    "pyarrow": lambda src, dst: pq.write_table(pacsv.read_csv(src), dst),
    "pandas": lambda src, dst: pd.read_csv(src).to_parquet(dst),
}


def read_back(path: Path) -> tuple[dict[str, list], bool, str]:
    """The checked columns as Python values, whether the `+05:30` offsets survived, and
    a note when pyarrow cannot read the file (then Polars reads it)."""
    columns = [*CHECKED, "created_at"]
    try:
        table = pq.read_table(path, columns=columns)
    except pa.ArrowException as e:
        frame = pl.read_parquet(path, columns=columns)
        values = {c: frame[c].to_list() for c in CHECKED}
        dtype = frame["created_at"].dtype
        kept = dtype == pl.String or (isinstance(dtype, pl.Datetime) and dtype.time_zone not in (None, "UTC"))
        return values, kept, f"pyarrow cannot read the file ({' '.join(str(e).split())[:70]}); read back with Polars"
    return {c: table.column(c).to_pylist() for c in CHECKED}, offset_kept(table.column("created_at").type), ""


def as_text(v: object) -> str | None:
    if v is None:
        return None
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, Decimal):
        return format(v, "f")
    return str(v)


def offset_kept(t: pa.DataType) -> bool:
    """Whether the +05:30 of the timestamps survives (as text or as the column's zone)."""
    if pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_string_view(t):
        return True
    return pa.types.is_timestamp(t) and t.tz not in (None, "UTC", "+00:00", "Etc/UTC")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--rows", type=int, default=2_000_000)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--dir", type=Path, default=Path(__file__).resolve().parent / "bench-data")
    args = parser.parse_args()
    work = args.dir
    work.mkdir(parents=True, exist_ok=True)

    src = work / f"export-{args.rows}.csv"
    if not src.exists():
        make_export(src, args.rows)
    original = {c: [] for c in CHECKED}
    with open(src, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            for c in CHECKED:
                original[c].append(row[c])
    print(f"input: {src.name}, {src.stat().st_size / 2**20:.1f} MiB, {args.rows:,} rows\n")

    results = []
    for name, convert in TOOLS.items():
        dst = work / f"out-{name.lower()}.parquet"
        times = []
        for _ in range(args.runs):
            gc.collect()
            t = time.perf_counter()
            convert(src, dst)
            times.append(time.perf_counter() - t)
        values, kept, note = read_back(dst)
        changed = {c: sum(a != as_text(b) for a, b in zip(original[c], values[c])) for c in CHECKED}
        results.append((name, min(times), dst.stat().st_size, changed, kept, note))

    print("| | " + " | ".join(r[0] for r in results) + " |")
    print("|---|" + "---:|" * len(results))
    print(f"| time (best of {args.runs}) | " + " | ".join(f"{r[1]:.2f} s" for r in results) + " |")
    print("| Parquet size | " + " | ".join(f"{r[2] / 2**20:.1f} MiB" for r in results) + " |")
    for c, label in CHECKED.items():
        print(f"| {label} changed | " + " | ".join(f"{r[3][c]:,}" for r in results) + " |")
    print("| `+05:30` offsets kept | " + " | ".join("yes" if r[4] else "no" for r in results) + " |")
    for r in results:
        if r[5]:
            print(f"\n{r[0]}: {r[5]}")

    messy = work / "messy.csv"
    make_messy(messy, 1_000_000)
    print(f"\n{messy.name}: 1,000,000 rows, one with a field too many and one with a quote never closed\n")
    print("| tool | result |")
    print("|---|---|")
    for name, convert in TOOLS.items():
        dst = work / f"messy-{name.lower()}.parquet"
        rejected = work / f"{dst.name}.rejected.csv"  # where vegg sets malformed rows aside
        for old in (dst, rejected):
            old.unlink(missing_ok=True)
        try:
            convert(messy, dst)
            result = f"{pq.read_metadata(dst).num_rows:,} rows written"
            if rejected.exists():
                kept = len(rejected.read_text(encoding="utf-8").splitlines()) - 1
                result += f", {kept} malformed rows set aside in {rejected.name}"
        except Exception as e:  # each tool raises its own error types
            result = f"error: {' '.join(str(e).split())[:80]}"
            if dst.exists():
                try:
                    result += f"; left a partial file of {pq.read_metadata(dst).num_rows:,} rows"
                except pa.ArrowException:
                    result += f"; left a broken file of {dst.stat().st_size:,} bytes"
        print(f"| {name} | {result} |")


if __name__ == "__main__":
    main()
