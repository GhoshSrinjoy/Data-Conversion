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
  - re-planning when a value late in the file does not fit the inferred type,
    for CSV and JSON Lines alike;
  - Windows-1252 fallback for invalid UTF-8;
  - quarantine of malformed rows (`--bad-lines`), which never replaces the input
    or an existing file without `--force`;
  - a stray quote costs only its own line, whether it is never closed or pairs
    up with a later quote across lines, and quotes inside fields are literal
    characters;
  - atomic outputs, including splits (parts are published together) and Ctrl-C
    in the standalone binary.
- **Exact chunking.** Chunk boundaries follow the tokenizer's quote rules,
  escape characters included, so a record is never cut in two whatever the
  chunk size.
- **Lossless JSON.** Integers beyond int64 and numbers with more digits than a
  double holds become decimals; keys keep their order; mixed-type fields keep
  their JSON text and are written back as JSON, so JSON → Parquet → JSON returns
  the same values; NaN and Infinity survive a round trip.
- **The same text from every engine**, whatever the column type: floats in their
  shortest exact digits (float32 and float16 in their own precision), dates and
  timestamps at any unit, nested values and maps as JSON, and nulls as empty
  fields. `schema`, `head`, `info` and reports name types the same way too
  (`string`, `float64`, `timestamp[ms,+05:30]`, `list<int64>`).
- **Hardened decoders.** Corrupt DCZ and packed data fail with an integrity
  error: every length is checked against the block index and the file size,
  and decompressed sizes and allocations are bounded.
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
- **Formats:** JSON arrays, ORC, Excel (read), Arrow IPC files and streams, and
  compressed inputs and outputs (`.gz`, `.zst`, `.xz`, `.bz2`, `.lz4`, `.br`,
  `.zip`). `compress` archives any file byte for byte, an already compressed one
  included.
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
