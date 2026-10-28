*"My CSV had 12 million rows. I converted it to Parquet and suddenly felt rich in storage space."*

# dataconv: fast, lossless data conversion and compression

Convert, compress, split, inspect and verify CSV, TSV, JSON Lines, JSON, Parquet,
Arrow IPC/Feather, ORC, Excel and DCZ files, from the command line or Python.

- **Lossless by design.** Zip codes keep their leading zeros, 20-digit IDs stay
  exact, `NA` is only a missing value where the column is numeric, timestamps
  keep their UTC offset, and malformed rows are set aside instead of dropped.
- **Fast.** A native Rust engine parses, types and writes in parallel. On a
  2-million-row CSV it is about 8x faster than the pandas path this project used
  to take, and it produces a smaller file.
- **Adaptive.** Threads, memory and chunk sizes follow the machine it runs on,
  including CPU and memory limits inside containers.
- **Strong compression.** DCZ, a byte-exact columnar format for delimited text,
  produced a file about 1.5x smaller than Parquet and 1.6x smaller than xz on a
  typical export, and still decompresses at about 1 GB/s.
- **Runs anywhere.** One Python package with no required dependencies, an
  optional native engine for Windows, macOS and Linux (x86-64 and ARM), and a
  standalone binary that needs no Python at all.

## Contents

- [Why this exists](#why-this-exists)
- [Install](#install)
- [Quick start](#quick-start)
- [What is the best compression for my data?](#what-is-the-best-compression-for-my-data)
- [Lossless type inference](#lossless-type-inference)
- [Robustness](#robustness)
- [Performance and resources](#performance-and-resources)
- [Command reference](#command-reference)
- [Python API](#python-api)
- [Engines and portability](#engines-and-portability)
- [Architecture](#architecture)
- [Development](#development)
- [License](#license)

## Why this exists

Converting a CSV sounds trivial until the data is real. On a 2-million-row,
135.5 MB export (ids, timestamps, user names, prices, zip codes, country codes),
the classic `pandas.read_csv(...).to_parquet(...)` approach and dataconv give:

|                                            | pandas + pyarrow | dataconv (native) |
|--------------------------------------------|-----------------:|------------------:|
| time                                       | 1.65 s           | **0.21 s**        |
| Parquet size                               | 51.1 MB          | **33.5 MB**       |
| zip codes that lost their leading zero     | 199,609          | **0**             |
| `NA` (Namibia) turned into a missing value | 250,549          | **0**             |

Measured on a 24-thread Windows machine (sizes in MB of 2^20 bytes, as dataconv
prints them). Your timings will differ; the losses will not.

Other problems it handles for you:

- **Files bigger than memory.** Input streams through in chunks sized to the
  memory budget.
- **A type change late in the file** (an integer column that turns into floats
  at row 9 million). The job re-plans the schema and restarts.
- **Encodings.** Windows-1252 and UTF-16 exports and stray bytes are detected,
  and the job falls back automatically.
- **Semicolon CSVs, preamble lines, missing or duplicate headers** are detected.
- **Interrupted jobs** never leave half-written files.

## Install

From a checkout of this repository:

```bash
pip install .                      # the package: pure Python, no dependencies
pip install ./native/python        # the native engine (needs a Rust toolchain)
pip install ".[arrow,fast]"        # pyarrow, numpy and extra codecs (optional)
```

Once the packages are published, the same is:

```bash
pip install "dataconv[native]"     # recommended: the Rust engine
pip install "dataconv[all]"        # everything: native, pyarrow, codecs, Excel
```

The standalone binary (no Python) builds with `cargo build --release -p
dataconv-cli` in `native/` and lands in `native/target/release/dataconv`.

Requirements: Python 3.8 or newer (the native engine needs 3.9 or newer). Check
what your machine offers with `dataconv doctor`.

## Quick start

```bash
dataconv convert data.csv data.parquet                 # lossless, parallel, tuned
dataconv convert data.csv --to parquet --profile max   # smallest Parquet
dataconv convert exports/ --to parquet -o lake/        # a whole tree, in parallel
dataconv compress huge.csv                             # huge.csv.dcz, byte-exact
dataconv decompress huge.csv.dcz                       # the original bytes again
dataconv split big.csv parts/ --size 100MB --to parquet
dataconv info data.parquet                             # rows, schema, codecs, stats
dataconv head data.csv -n 20
dataconv verify data.csv data.parquet                  # same data?
dataconv bench data.csv                                # which format suits this data
```

```python
import dataconv as dc

report = dc.convert("data.csv", "data.parquet")
print(report)               # rows, sizes, ratio, speed, engine
print(report.warnings)      # e.g. "column 'zip' kept as text: leading zeros ..."

dc.compress("data.csv")                 # -> data.csv.dcz
blob = dc.pack([19.99, 5.25, 7.5])      # compress a list in memory
dc.unpack(blob)                         # -> [19.99, 5.25, 7.5], bit for bit

table = dc.load("data.csv")             # a pyarrow.Table with lossless types
dc.write(table, "out.parquet", profile="max")
```

## What is the best compression for my data?

It depends on what you will do with the file. `dataconv bench yourfile.csv`
measures every option on your own data. For the 135.5 MB export above:

| format            | size    | ratio | write   | read   | use it for |
|-------------------|--------:|------:|--------:|-------:|------------|
| **DCZ max**       | 21.1 MB | 6.4x  | 16 s    | 0.3 s  | archival, smallest |
| **DCZ balanced**  | 21.9 MB | 6.2x  | 0.3 s   | 0.1 s  | lossless storage and transfer |
| Parquet max       | 29.3 MB | 4.6x  | 3.8 s   | 0.13 s | analytics, smallest Parquet |
| Parquet zstd-3    | 33.5 MB | 4.0x  | 0.2 s   | 0.15 s | analytics (default) |
| csv.xz            | 35.6 MB | 3.8x  | 7.6 s   | 1.8 s  | compatibility with xz tools |
| csv.zst           | 44.0 MB | 3.1x  | 0.9 s   | 0.3 s  | fast generic compression |
| Parquet snappy    | 49.6 MB | 2.7x  | 0.4 s   | 0.11 s | old readers |
| csv.gz            | 49.6 MB | 2.7x  | 2.4 s   | 0.4 s  | maximum compatibility |

Rules of thumb:

- **To query the data** (pandas, polars, DuckDB, Spark), use **Parquet**. The
  default zstd profile is the right speed/size balance. `--profile max` tunes each
  column's encoding by trial: delta encoding for sorted integers and timestamps,
  byte-stream-split for floats, prefix encoding for IDs and URLs.
- **To store or ship a CSV exactly**, use **DCZ**. It is lossless to the byte
  (every block is verified when written and checksummed when read), so a DCZ
  file can stand in for the original CSV.
- **Numbers in memory** (lists, arrays, time series) go through `dc.pack`.

### How DCZ gets there

General-purpose compressors see a CSV as a stream of characters. DCZ parses it,
remembers exactly how every record was written (quoting and line endings), and
encodes each column with the representation that suits it:

| column looks like            | stored as                                                         |
|------------------------------|-------------------------------------------------------------------|
| `42`, `-7`, `1700000000`     | 64-bit integers: delta or run-length coded, minimal byte width     |
| `19.99`, `0.05`, `-3.5`      | exact decimals: integer mantissa plus scale (no float rounding)    |
| `2024-03-07 12:05:00`, `INV-000123` | a template of literal text and numeric fields, each delta-coded |
| `red`, `green`, `red`        | a dictionary and small integer indices                             |
| anything else                | lengths and bytes, kept apart                                      |

Integers are stored as **byte planes** (all low bytes, then all next bytes, and
so on), so the nearly constant high bytes compress to almost nothing. Cells that
fit none of the patterns, such as `007` in an integer column or a ragged row,
are kept verbatim. The result goes through zstd, or for `max` the best of zstd,
lzma and brotli per stream. The format is specified in
[docs/FORMAT.md](docs/FORMAT.md).

### Lists and arrays: `dc.pack`

`dc.pack` applies the same ideas to in-memory values and restores them exactly,
including NaN payloads, `-0.0`, numpy dtypes and shapes. For one million values:

| data                          | json + gzip-9 | pickle + zstd-19 | numpy + xz-9 | **dc.pack** |
|-------------------------------|--------------:|-----------------:|-------------:|------------:|
| prices with 2 decimals        | 2.6 MB        | 2.8 MB           | 2.1 MB       | **1.9 MB**  |
| timestamps, 1 s steps         | 1.6 MB        | 874 KB           | 434 KB       | **261 KB**  |
| smooth sensor readings        | 2.0 MB        | 2.2 MB           | 1.5 MB       | **1.1 MB**  |
| small counts                  | 728 KB        | 614 KB           | 592 KB       | **556 KB**  |
| random doubles                | 8.4 MB        | 6.7 MB           | 7.0 MB       | **6.5 MB**  |
| IDs like `ORD-00000001`       | 2.3 MB        | 242 KB           | -            | **81 bytes** |

Floats that were decimals before they became doubles (prices, sensor readings)
are stored as integer mantissas, which only happens when dividing by the power of
ten reproduces the exact bits. Other floats use XOR-delta byte planes. Random
doubles are close to incompressible for every method.

## Lossless type inference

A column gets a typed representation only when every value converts without
losing information. Otherwise it stays text, and the report says why.

| values                                     | inferred type          | why |
|--------------------------------------------|------------------------|-----|
| `1`, `-2`, `30`                             | `int64`                | |
| `007`, `02134`                              | `string`               | leading zeros carry meaning |
| `+4915112345678`                            | `string`               | a `+` sign usually means a phone number |
| `18446744073709551615`                      | `decimal(20,0)`        | beyond int64; a float would round it |
| `0.1`, `2.50`, `1e-5`                       | `float64`              | every value round-trips |
| `1.00000000000000000001`                    | `decimal(21,20)`       | more digits than a double holds |
| `2024-01-02`                                | `date`                 | |
| `2024-01-02T03:04:05.123+05:30`             | `timestamp[ms,+05:30]` | unit from the digits, offset kept |
| offsets `+05:30` and `Z` mixed in one column | `string`              | a single type would drop local times |
| `true`, `False`                             | `bool`                 | |
| `NA`, `null`, `-`, empty in a numeric column | missing value          | |
| `NA` in a text column (Namibia)             | `"NA"`                 | kept as written |

An unquoted empty field is missing; a quoted empty field `""` is an empty string,
and the distinction survives a round trip. Options:

- `--types string` keeps every column as text;
- `--types loose` behaves like most tools and parses `007` as `7`;
- `--schema "zip=string,amount=decimal(12,2),ts=timestamp[us,UTC]"` overrides
  single columns.

Types are inferred from a sample (the start of the file plus windows spread
through it). If a later value does not fit, the job re-plans from a full scan
and restarts; `--infer full` scans everything up front.

JSON Lines and JSON follow the same rules. An integer within int64 stays an
integer, a longer one becomes `decimal(p,0)`, and a number with more digits than
a double holds becomes a decimal (or text, beyond 38 digits). Keys keep their
order in the file. A field that mixes strings, numbers and objects keeps every
value as its JSON text, so `1` and `"1"` stay different. Such columns are marked,
and JSON output writes them back as the JSON they hold, so JSON → Parquet → JSON
returns the same values. `NaN` and `Infinity` are written as the strings `"NaN"`
and `"Infinity"`, and read back as floats.

## Robustness

- **Bad rows.** Rows with the wrong number of fields go to
  `<output>.rejected.csv` by default (`--bad-lines quarantine`). Alternatives
  are `skip`, `error` and `pad` (pad short rows with missing values). The
  quarantine file is never the input, and it replaces an existing file only with
  `--force`. A clean run with `--force` removes the rejected rows of an earlier
  run.
- **Stray quotes.** A quote inside a field (`55" screen`) is a literal
  character. A quote that is never closed costs only its own line: that line is
  a malformed row and parsing resumes on the next one, instead of the rest of
  the file becoming one value. The same holds when a stray quote pairs up with a
  later quote that has text after it (`"Smith, John"` further down), as a
  quoted field spanning lines never does. Only a later quote that ends a field
  (`55"` at the end of a value) closes it: the lines in between are then one
  value, which is valid CSV. They become a rejected row unless that row happens
  to have the right number of fields, and the warning says how many lines it
  holds. A genuine quoted field may run to 1 GiB; a quote still open after that
  is taken to be a stray, and the report warns about it.
- **Encodings.** UTF-8 with or without BOM, UTF-16 and Windows-1252 are detected
  from samples. Invalid bytes found later in the file trigger an automatic
  re-read as Windows-1252. `--encoding latin-1` means ISO-8859-1 itself, where
  every byte is the character of the same number.
- **Dialects.** The delimiter (`,` `;` `\t` `|` and more), quoting, backslash
  escapes, line endings, header presence and preamble lines before the header are
  detected. A single column whose values contain a delimiter (timestamps, MAC
  addresses, URLs with ports, `key:value` pairs) keeps its header and its values
  whole. Header names are kept exactly as written; duplicate or empty names are
  made unique. In a single-column file a blank line is a row with a missing
  value. Options that cannot work (an unknown encoding, a delimiter that is also
  the quote character) are refused up front.
- **Compressed inputs.** `.gz`, `.zst`, `.xz`, `.bz2`, `.lz4`, `.br` and
  single-file `.zip` inputs are read transparently, including multi-member gzip
  and concatenated frames, and outputs such as `.csv.gz` or `.jsonl.zst` are
  compressed on the way out. A truncated or corrupt stream is an error, never a
  shorter result.
- **Atomic outputs.** Results are written to a temporary file and moved into
  place only on success. Ctrl-C or an error leaves the destination untouched.
  `split` writes its parts into a private folder and publishes them together,
  so a failed or restarted split never leaves some parts behind.
- **Readable everywhere.** Timestamps and times in seconds are stored in Parquet
  as milliseconds, and date64 values as dates, so every reader sees the right
  type rather than bare integers.
- **Files from other tools.** Parquet and Arrow columns of any type become the
  same CSV and JSON text with every engine. Floats are written with the shortest
  digits that read back exactly (float32 and float16 in their own precision).
  Times are written at any unit and year, and structs, lists and maps become
  JSON. A map stays an object even with repeated or non-text keys (read back,
  JSON objects become structs, and a repeated key keeps its last value).
  pyarrow's `timestamp[s]` columns keep their time zone. Feather version 1 files
  are read by the arrow engine only.
- **Verification.** `dataconv verify a b` compares contents across formats and
  reports the first differences; DCZ checks a CRC-32 per block and for the
  whole file.

## Performance and resources

The native engine splits input into record-aligned chunks (quote-aware, so
newlines inside quoted fields are safe). It decodes the chunks on all cores,
encodes Parquet columns and row groups in parallel, and overlaps reading,
decoding and writing. Memory stays bounded however large the input is.

Everything adapts to the machine:

| resource      | detected from                                                  | drives |
|---------------|----------------------------------------------------------------|--------|
| CPUs          | affinity masks and cgroup CPU quotas (Docker, Kubernetes)      | worker threads, parallel files |
| memory        | available RAM, cgroup limits, 32-bit address space             | chunk size, chunks in flight, row group size |
| data          | sampled field widths, cardinality, measured output size        | dictionary use, encodings, split sizes |

Override with `--threads`, `--memory 2GB`, or the `DATACONV_THREADS` and
`DATACONV_MEMORY` environment variables. `dataconv doctor` shows what was
detected, and `DATACONV_TRACE=1` prints phase timings for a job.

Directory conversion runs several files at once when they are small and gives
large files all the threads. `--skip-existing` makes repeated runs incremental.

## Command reference

```text
dataconv convert SRC [DST] [options]   convert a file or a directory tree
dataconv compress SRC [DST]            lossless DCZ compression of any file, byte for byte
dataconv decompress SRC [DST]          restore the original bytes (a .gz stays a .gz)
dataconv split SRC DIR --rows N | --size SIZE [--to FORMAT] [--pattern P]
dataconv merge SRC... -o DST           concatenate, unifying schemas
dataconv info FILE [--exact]           format, rows, schema, codecs, statistics
dataconv head FILE [-n N]              first rows
dataconv count FILE                    exact row count
dataconv schema FILE                   inferred column types
dataconv sniff FILE                    detected CSV dialect
dataconv stats FILE                    per-column profile
dataconv verify A B                    same data? (exit code 1 if not)
dataconv bench FILE [--quick]          compare formats and codecs on this data
dataconv doctor                        resources, engines and codecs
```

Frequently used options:

| option | meaning |
|--------|---------|
| `--to FORMAT` | csv, tsv, psv, jsonl, json, parquet, ipc (Feather), orc, dcz |
| `-p, --profile` | `fast`, `balanced` (default), `max` |
| `-c, --compression`, `--level` | codec and level, e.g. `-c zstd --level 9` |
| `--types`, `--schema` | lossless (default), all text, permissive; per-column overrides |
| `-d, --delimiter`, `--encoding`, `--no-header`, `--skip-rows` | override detection |
| `--columns a,b`, `--limit N` | select columns, convert a prefix |
| `--bad-lines` | `quarantine` (default), `skip`, `error`, `pad` |
| `--include`, `--exclude`, `--skip-existing`, `--workers` | directory conversion |
| `--threads`, `--memory`, `--engine` | resources and engine choice |
| `-f, --overwrite`, `--json`, `-q`, `-v` | output control |

### Coming from the original scripts

The three scripts this project started with are retired. Each job is one
command now:

| before | now |
|--------|-----|
| `python csv_to_parquet.py data.csv` | `dataconv convert data.csv --to parquet` |
| `python csv_to_parquet.py folder --output out` | `dataconv convert folder --to parquet -o out --include "*.csv"` |
| `python parquet_viewer.py data.parquet --rows 5` | `dataconv info data.parquet`, then `dataconv head data.parquet -n 5` |
| `python csv_splitter_converter.py big.csv parts --chunk-size 250` | `dataconv split big.csv parts --size 250MB --to parquet --pattern "chunk_{n:04}"` |

## Python API

| function | purpose |
|----------|---------|
| `convert(src, dst=None, **options)` | convert a file, or a directory (returns `Report` / `BatchReport`) |
| `convert_dir(src, dst, to=..., include=..., skip_existing=...)` | directory trees |
| `split(src, out_dir, rows=None, size=None, to=None, pattern=...)` | split by rows or target size |
| `merge(sources, dst)` | concatenate with schema unification |
| `compress(src, dst=None, profile=...)` / `decompress(src, dst=None)` | DCZ |
| `pack(values, profile=...)` / `unpack(blob)` | lists, tuples, `array.array`, numpy arrays |
| `load(path, **options)` / `write(data, dst, **options)` | pyarrow tables; `write` also takes pandas, polars and lists of dicts |
| `info`, `head`, `count`, `schema`, `sniff`, `stats` | inspection |
| `verify(a, b)` | content comparison across formats |
| `bench(src, quick=False)` | measure formats and codecs, with recommendations |
| `system_info()`, `plan(size)` | what the machine offers, how a job would use it |

All options of the `convert` command are keyword arguments: `to`, `profile`,
`compression`, `level`, `types`, `schema`, `delimiter`, `encoding`, `header`,
`skip_rows`, `null_values`, `bad_lines`, `quarantine`, `columns`, `limit`,
`threads`, `memory`, `overwrite`, `engine`, and `progress` (`True` for a progress
bar, or a callable `(done_bytes, total_bytes)` that returns `False` to cancel).
Errors derive from `dataconv.DataconvError`.

## Engines and portability

| engine | needs | reads and writes | speed |
|--------|-------|------------------|-------|
| `native` | the `dataconv-native` wheel (CPython 3.9+, one abi3 wheel per platform) | CSV/TSV, JSON Lines, Parquet, Arrow IPC, DCZ | fastest |
| `arrow` | pyarrow | all of the above plus JSON arrays, ORC and Excel (needs openpyxl) | fast |
| `pure` | nothing (Python 3.8+) | CSV/TSV, JSON Lines, JSON, DCZ | portable |

`--engine auto` (the default) picks the first engine that handles both formats.
All engines apply the same type rules and read and write the same DCZ files,
which the test suite cross-checks. The suite runs on Python 3.8 to 3.13, with and
without each optional package; set `DATACONV_DISABLE=all` (or a list such as
`dataconv_native,pyarrow`) to try the fallbacks yourself.

The native engine is plain Rust with no system dependencies. It builds for
Windows, macOS and Linux on x86-64 and ARM64, and from source wherever Rust runs.

## Architecture

```text
dataconv (Python)                         dataconv-native (Rust, PyO3, abi3)
  cli.py      command line                  python/   bindings (GIL released, Ctrl-C aware)
  api.py      public API, engine choice     core/     the engine
  engines/    native | arrow | pure           chunk.rs     quote-aware record-aligned chunks
  infer.py    lossless type rules             tokenize.rs  zero-copy CSV tokenizer
  _sniff.py   dialect detection               types.rs     lossless type rules
  dcz.py      DCZ reader/writer               decode.rs    guarded typed column builders
  resources.py container-aware planning       convert.rs   streaming pipeline, re-planning
  tune.py     Parquet tuning                  tables.rs    parallel Parquet, IPC, JSONL, CSV
  _csvtok.py  CSV chunks and records          jsonl.rs     lossless JSON Lines
                                              split.rs     byte-exact and size-targeted splits
                                              dcz/         columnar codec, container, int coding
                                              pack.rs      value packing
                                            cli/      standalone `dataconv` binary
```

## Development

```bash
pip install -e ".[dev,arrow,fast]"
pip install ./native/python             # or: cd native/python && maturin develop --release
pytest                                  # Python suite (all engines available)
DATACONV_DISABLE=all pytest             # the pure-Python fallbacks only
cd native && cargo test                 # Rust unit and integration tests
```

Changes to the type rules, the CSV and JSON rules or DCZ must be made in both
implementations; `tests/test_infer.py`, `tests/test_csv_rules.py`,
`tests/test_json_rules.py` and `tests/test_dcz.py` check that they agree.

Release artifacts come from the release workflow. To build them on your own
machine, run `python native/build_release.py`. Rust embeds the source path of
every crate in panic messages, so a plain build names your home directory. The
script remaps the paths of the build machine (home directory, cargo home,
checkout), builds the binary and the wheel into `dist/`, and fails if either
still contains such a path. `python native/build_release.py --check FILE...`
checks existing artifacts.

## License

PolyForm Noncommercial License 1.0.0. See [LICENSE](LICENSE).

You may use, modify and share this software for any **noncommercial**
purpose: research, education, personal projects and hobby use, and use by
charitable, educational, public research, public safety, health and government
organizations. **Commercial use is not permitted** without a separate license
from the author. Earlier releases were published under Apache-2.0 and remain
available under those terms.
