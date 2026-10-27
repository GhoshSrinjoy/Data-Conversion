# Changelog

## 1.0.0

A rewrite of the three original scripts into one package, `dataconv`.

### Added

- **Native engine** (`dataconv-native`, Rust):
  - a quote-aware chunker and zero-copy tokenizer;
  - parallel, memory-bounded conversion between CSV/TSV, JSON Lines, Parquet,
    Arrow IPC and DCZ;
  - Parquet writing that encodes columns and row groups in parallel and tunes
    encodings per column;
  - a standalone `dataconv` binary that needs no Python.
- **Lossless type inference**, shared by every engine: canonical integers only
  (leading zeros and `+` signs keep text), exact decimals for values beyond
  int64 or float64 precision, timestamps with unit and UTC offset, and
  missing-value markers only in typed columns.
- **Recovery** instead of failure:
  - re-planning when a value late in the file does not fit the inferred type;
  - Windows-1252 fallback for invalid UTF-8;
  - quarantine of malformed rows (`--bad-lines`);
  - atomic outputs.
- **DCZ**, a byte-exact, block-parallel columnar compressor for delimited text,
  with a pure-Python reader and writer
  ([format specification](docs/FORMAT.md)), and `pack`/`unpack` for lists and
  numpy arrays.
- **Resource-aware planning.** Threads and memory come from CPU affinity, cgroup
  quotas and available memory; `dataconv doctor` and `plan()` show them.
- **New commands:** `convert` for files and whole directory trees, `split` (by
  rows or measured target size, byte-exact for CSV), `merge` (schema
  unification), `info`, `head`, `count`, `schema`, `sniff`, `stats`, `verify`,
  `bench`, `compress`, `decompress` and `doctor`, each with `--json` output.
- **Formats:** JSON arrays, ORC, Excel (read), and compressed inputs and outputs
  (`.gz`, `.zst`, `.xz`, `.bz2`, `.lz4`, `.br`, `.zip`).
- **Engines:** native, pyarrow and pure Python, chosen automatically. The pure
  engine runs on Python 3.8+ with no dependencies.

### Changed

- `csv_to_parquet.py`, `parquet_viewer.py` and `csv_splitter_converter.py` now
  call the new engine. Their arguments and output names are unchanged, but
  conversions keep leading zeros, `NA` strings and large integers that the old
  pandas-based code altered.
- `csv_splitter_converter.py` now hits the requested chunk size (measured on the
  output, not estimated from memory usage) and moves malformed rows to a
  `.rejected.csv` file instead of dropping them silently.
- The license is now the PolyForm Noncommercial License 1.0.0. Earlier releases
  remain under Apache-2.0.
