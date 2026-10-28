# vegg-core

The engine behind [vegg](https://github.com/GhoshSrinjoy/vegg): lossless,
parallel, memory-bounded conversion between CSV/TSV, JSON Lines, Parquet, Arrow
IPC and DCZ, with type inference that never changes a value.

Most people want one of its front ends instead:

- the command-line program: `cargo install vegg`;
- the Python package: `pip install vegg`.

This crate is the library both of them call. Its API follows the needs of those
two front ends and may change between minor versions.

Licensed under the PolyForm Noncommercial License 1.0.0.
