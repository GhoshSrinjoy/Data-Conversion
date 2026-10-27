# dataconv-native

The native (Rust) engine behind [dataconv](../../README.md). Install it next to
`dataconv` and every conversion, compression and inspection command picks it up
automatically:

```bash
pip install dataconv dataconv-native
```

It is a single `abi3` wheel per platform (CPython 3.9+), with no runtime
dependencies. Without it, dataconv falls back to pyarrow or pure Python.

Licensed under the PolyForm Noncommercial License 1.0.0.
