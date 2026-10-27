"""Command-line interface: ``dataconv <command> ...`` (also ``python -m dataconv``)."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from typing import Any, Dict, List, Optional, Sequence

from . import __version__
from ._util import format_count, format_size
from .errors import DataconvError

EPILOG = """examples:
  dataconv convert data.csv data.parquet              lossless types, parallel, auto-tuned
  dataconv convert data.csv --to parquet --profile max
  dataconv convert exports/ --to parquet -o lake/     a whole directory tree, in parallel
  dataconv compress huge.csv                          huge.csv.dcz, restores byte-exact
  dataconv split big.csv parts/ --size 100MB --to parquet
  dataconv info data.parquet                          schema, row groups, codecs, stats
  dataconv head data.csv -n 20
  dataconv bench data.csv                             which format/codec suits this data
  dataconv doctor                                     what this machine can do
"""


def _table(headers: Sequence[str], rows: Sequence[Sequence[Any]], max_width: Optional[int] = None) -> str:
    cells = [[("" if v is None else str(v)) for v in row] for row in rows]
    width = max_width or shutil.get_terminal_size((120, 20)).columns
    ncols = len(headers)
    widths = [len(h) for h in headers]
    for row in cells:
        for i in range(ncols):
            if i < len(row):
                widths[i] = max(widths[i], len(row[i].replace("\n", "\\n")))
    cap = max(8, (width - 2 * ncols) // max(1, ncols)) if sum(widths) + 2 * ncols > width else 1 << 30
    widths = [min(w, max(cap, len(h))) for w, h in zip(widths, headers)]

    def fit(text: str, w: int) -> str:
        text = text.replace("\n", "\\n")
        return text if len(text) <= w else text[: max(1, w - 1)] + "~"

    lines = ["  ".join(fit(h, w).ljust(w) for h, w in zip(headers, widths)).rstrip(),
             "  ".join("-" * w for w in widths)]
    for row in cells:
        lines.append("  ".join(fit(row[i] if i < len(row) else "", w).ljust(w) for i, w in enumerate(widths)).rstrip())
    return "\n".join(lines)


def _print_json(data: Any) -> None:
    print(json.dumps(data, indent=2, default=str, ensure_ascii=False))


def _report(report: Any, args: argparse.Namespace) -> None:
    if args.json:
        _print_json(report.to_dict())
        return
    if args.quiet:
        return
    print(report.summary())
    for w in getattr(report, "warnings", []) or []:
        print(f"  note: {w}", file=sys.stderr)
    for src, err in getattr(report, "failed", []) or []:
        print(f"  failed: {src}: {err}", file=sys.stderr)
    if args.verbose and getattr(report, "schema", None):
        print(_table(["column", "type"], report.schema))


def _common(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("resources")
    g.add_argument("--threads", type=int, help="worker threads (default: all usable CPUs)")
    g.add_argument("--memory", help="memory budget, e.g. 2GB (default: half the available RAM)")
    g.add_argument("--engine", choices=["auto", "native", "arrow", "pure"], default="auto")


def _reading(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("reading")
    g.add_argument("--from", dest="from_", metavar="FORMAT", help="input format when the extension does not say")
    g.add_argument("--delimiter", "-d", help="field delimiter (default: detected); use \\t for tabs")
    g.add_argument("--quote", help="quote character (default: \")")
    g.add_argument("--escape", help="escape character inside quotes, e.g. \\")
    g.add_argument("--encoding", help="text encoding (default: detected)")
    hdr = g.add_mutually_exclusive_group()
    hdr.add_argument("--header", dest="header", action="store_true", default=None, help="first row is a header")
    hdr.add_argument("--no-header", dest="header", action="store_false", help="first row is data")
    g.add_argument("--skip-rows", type=int, help="lines to skip before the header (default: detected)")
    g.add_argument("--types", choices=["infer", "string", "loose"], default="infer",
                   help="infer: lossless typing (default); string: keep all text; loose: also parse 007 as 7")
    g.add_argument("--schema", help='type overrides, e.g. "zip=string,amount=decimal(12,2)"')
    g.add_argument("--null-values", help="extra comma-separated markers of missing values")
    g.add_argument("--columns", help="comma-separated columns to keep")
    g.add_argument("--limit", type=int, help="convert at most this many rows")
    g.add_argument("--bad-lines", choices=["quarantine", "skip", "error", "pad"], default="quarantine",
                   help="rows with the wrong field count (default: move them to <output>.rejected.csv)")
    g.add_argument("--quarantine", help="file for rejected rows")
    g.add_argument("--infer", choices=["sample", "full"], default="sample", help="infer types from a sample (default) or the whole file")


def _writing(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("writing")
    g.add_argument("--to", "-t", help="output format (csv, tsv, jsonl, json, parquet, ipc, orc, dcz)")
    g.add_argument("--profile", "-p", choices=["fast", "balanced", "max"], default="balanced",
                   help="speed/size trade-off (default: balanced)")
    g.add_argument("--compression", "-c", help="codec: zstd, snappy, lz4, gzip, brotli, none")
    g.add_argument("--level", type=int, help="codec level")
    g.add_argument("--row-group-rows", type=int, help="Parquet rows per row group")
    g.add_argument("--out-delimiter", help="delimiter for CSV output")
    g.add_argument("--crlf", action="store_true", help="CRLF line endings for CSV output")
    g.add_argument("--overwrite", "-f", action="store_true", help="replace existing outputs")


def _opts(args: argparse.Namespace, *names: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for n in names:
        v = getattr(args, n, None)
        if v is not None and v is not False:
            out[n] = v
    if getattr(args, "crlf", False):
        out["out_crlf"] = True
    return out


READ_OPTS = ("from_", "delimiter", "quote", "escape", "encoding", "header", "skip_rows", "types", "schema",
             "null_values", "columns", "limit", "bad_lines", "quarantine", "infer", "threads", "memory", "engine")
WRITE_OPTS = ("to", "profile", "compression", "level", "row_group_rows", "out_delimiter", "overwrite")


def cmd_convert(args: argparse.Namespace) -> int:
    from .api import convert

    opts = _opts(args, *READ_OPTS, *WRITE_OPTS)
    if os.path.isdir(args.src):
        opts.update(_opts(args, "include", "exclude", "skip_existing", "workers"))
        if args.no_recursive:
            opts["recursive"] = False
    dst = args.dst or args.output
    report = convert(args.src, dst, progress=args.progress, **opts)
    _report(report, args)
    return 0 if getattr(report, "ok", True) else 1


def cmd_compress(args: argparse.Namespace) -> int:
    from .api import compress

    report = compress(args.src, args.dst, profile=args.profile, threads=args.threads, block_size=args.block_size,
                      overwrite=args.overwrite, engine=args.engine, progress=args.progress)
    _report(report, args)
    return 0


def cmd_decompress(args: argparse.Namespace) -> int:
    from .api import decompress

    report = decompress(args.src, args.dst, threads=args.threads, overwrite=args.overwrite, engine=args.engine, progress=args.progress)
    _report(report, args)
    return 0


def cmd_split(args: argparse.Namespace) -> int:
    from .api import split

    if args.rows is None and args.size is None:
        raise DataconvError("give --rows N or --size SIZE")
    opts = _opts(args, *READ_OPTS, "profile", "compression", "level", "overwrite")
    report = split(args.src, args.out_dir, rows=args.rows, size=args.size, to=args.to, pattern=args.pattern,
                   extension=args.extension, progress=args.progress, **opts)
    if args.json:
        _print_json(report.to_dict())
        return 0
    if not args.quiet:
        print(report.summary())
        for part in report.parts:
            print(f"  {part['path']}: {format_count(part['rows'])} rows, {format_size(part['bytes'])}")
    return 0


def cmd_merge(args: argparse.Namespace) -> int:
    from .api import merge

    report = merge(args.sources, args.output, **_opts(args, "to", "profile", "compression", "level", "overwrite", "types", "schema"))
    _report(report, args)
    return 0


def cmd_info(args: argparse.Namespace) -> int:
    from .api import info

    data = info(args.file, exact=args.exact, **_opts(args, *READ_OPTS))
    if args.json:
        _print_json(data)
        return 0
    size = data.get("file_size")
    print(f"{data.get('path')}  [{data.get('format')}]  {format_size(size)}")
    for key in ("rows", "row_groups", "record_batches", "compression", "created_by", "kind", "profile",
                "original_size", "block_count", "columnar_blocks", "ratio", "engine"):
        if data.get(key) is not None:
            value = data[key]
            if key == "original_size":
                value = f"{format_size(value)} ({value:,} bytes)"
            elif key == "ratio":
                value = f"{value:.2f}x"
            elif key == "rows":
                value = format_count(value)
            print(f"  {key.replace('_', ' ')}: {value}")
    dialect = data.get("dialect")
    if dialect:
        delim = {"\t": "\\t"}.get(dialect.get("delimiter"), dialect.get("delimiter"))
        print(f"  dialect: delimiter={delim!r} encoding={dialect.get('encoding')} header={dialect.get('header')} "
              f"skip_rows={dialect.get('skip_rows')}")
    cols = data.get("columns") or []
    if cols:
        keys = [k for k in ("name", "type", "physical_type", "logical_type", "compression", "encodings",
                            "compressed_bytes", "uncompressed_bytes", "null_count") if any(k in c for c in cols)]
        rows = []
        for c in cols:
            row = []
            for k in keys:
                v = c.get(k)
                if isinstance(v, list):
                    v = ",".join(map(str, v))
                if k.endswith("_bytes") and v is not None:
                    v = format_size(v)
                row.append(v)
            rows.append(row)
        print(_table(keys, rows))
    return 0


def cmd_head(args: argparse.Namespace) -> int:
    from .api import head

    data = head(args.file, args.n, **_opts(args, *READ_OPTS))
    if args.json:
        _print_json(data)
        return 0
    print(_table(data["columns"], [[("" if v is None else v) for v in r] for r in data["rows"]]))
    if args.verbose:
        print()
        print(_table(["column", "type"], list(zip(data["columns"], data["types"]))))
    return 0


def cmd_count(args: argparse.Namespace) -> int:
    from .api import count

    n = count(args.file, **_opts(args, *READ_OPTS))
    print(json.dumps({"rows": n}) if args.json else n)
    return 0


def cmd_schema(args: argparse.Namespace) -> int:
    from .api import schema

    cols = schema(args.file, **_opts(args, *READ_OPTS))
    if args.json:
        _print_json([{"name": n, "type": t} for n, t in cols])
    else:
        print(_table(["column", "type"], cols))
    return 0


def cmd_sniff(args: argparse.Namespace) -> int:
    from .api import sniff

    d = sniff(args.file, **_opts(args, *READ_OPTS))
    if args.json:
        _print_json(d)
    else:
        for k in ("encoding", "delimiter", "quote", "escape", "terminator", "header", "skip_rows", "has_quotes"):
            print(f"{k:12s}{d.get(k)!r}")
        print(f"{'columns':12s}{', '.join(d.get('columns', []))}")
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    from ._stats import stats

    data = stats(args.file, top=args.top, **_opts(args, *READ_OPTS))
    if args.json:
        _print_json(data)
        return 0
    rows = []
    for c in data:
        top = ", ".join(f"{t['value']}({t['count']})" for t in c.get("top", [])[:3])
        rows.append([c["column"], c["type"], c["nulls"], c.get("distinct"), c.get("min", c.get("min_length")),
                     c.get("max", c.get("max_length")), "" if c.get("mean") is None else f"{c['mean']:.6g}", top])
    print(_table(["column", "type", "nulls", "distinct", "min", "max", "mean", "top values"], rows))
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    from ._verify import verify

    result = verify(args.a, args.b)
    if args.json:
        _print_json(result)
    elif result["equal"]:
        print("identical bytes" if result["identical_bytes"] else "same data")
    else:
        print("DIFFERENT")
        for d in result["differences"]:
            print(f"  {d}")
    return 0 if result["equal"] else 1


def cmd_bench(args: argparse.Namespace) -> int:
    from ._bench import bench, format_table

    result = bench(args.src, quick=args.quick, read=not args.no_read)
    if args.json:
        _print_json(result)
    else:
        print(f"{args.src}: {format_size(result['bytes_in'])}  (engines: {', '.join(result['engines'])})")
        print(format_table(result))
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    from .api import plan, system_info

    info = system_info()
    if args.json:
        info["plan_10GB"] = plan(10 << 30)
        _print_json(info)
        return 0
    print(f"dataconv {__version__}  (native engine: {info.get('native_version') or 'not installed'})")
    print(f"python   {info['python']} {info['implementation']} {info['bits']}-bit"
          f"{'' if info['gil'] else ' free-threaded'} on {info['os']} {info['os_release']} ({info['arch']})")
    limit = f", container limit {format_size(info['mem_limit'])}" if info.get("mem_limit") else ""
    physical = f" ({info['cpu_physical']} physical)" if info.get("cpu_physical") else ""
    where = " in a container" if info.get("container") else ""
    print(f"cpu      {info['cpu_usable']} usable of {info['cpu_logical']} logical{physical}{where}")
    print(f"memory   {format_size(info['mem_available'])} available of {format_size(info['mem_total'])}{limit}")
    print(f"engines  {', '.join(info['engines'])}")
    codecs = ", ".join(f"{k} ({v})" for k, v in info["codecs"].items() if v)
    print(f"codecs   {codecs}")
    missing = [k for k, v in info["codecs"].items() if not v]
    if missing:
        print(f"         unavailable: {', '.join(missing)}")
    p = plan(10 << 30)
    print(f"plan     a 10 GB job: {p['threads']} threads, {format_size(p['memory_budget'])} budget, "
          f"{format_size(p['chunk_size'])} chunks")
    tips = []
    if "native" not in info["engines"]:
        tips.append('install the native engine for 5-50x faster conversions: pip install "dataconv[native]"')
    if "arrow" not in info["engines"]:
        tips.append('install pyarrow for ORC/Excel/JSON-array support and in-memory tables: pip install "dataconv[arrow]"')
    for t in tips:
        print(f"tip      {t}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dataconv",
        description="Fast, lossless, resource-aware data conversion and compression.",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"dataconv {__version__}")
    out = argparse.ArgumentParser(add_help=False)
    out.add_argument("--json", action="store_true", help="machine-readable output")
    out.add_argument("-q", "--quiet", action="store_true")
    out.add_argument("-v", "--verbose", action="store_true")
    out.add_argument("--no-progress", dest="progress", action="store_false", default=None, help="hide the progress bar")
    sub = parser.add_subparsers(dest="command", metavar="command")

    p = sub.add_parser("convert", parents=[out], help="convert between formats (also whole directories)")
    p.add_argument("src")
    p.add_argument("dst", nargs="?")
    p.add_argument("-o", "--output", help="output file or directory")
    _reading(p)
    _writing(p)
    g = p.add_argument_group("directories")
    g.add_argument("--include", action="append", help="glob of files to convert (repeatable)")
    g.add_argument("--exclude", action="append", help="glob of files to skip (repeatable)")
    g.add_argument("--no-recursive", action="store_true", help="do not descend into subdirectories")
    g.add_argument("--skip-existing", action="store_true", help="skip outputs newer than their input")
    g.add_argument("--workers", type=int, help="files converted at once (default: adaptive)")
    _common(p)
    p.set_defaults(func=cmd_convert)

    p = sub.add_parser("compress", parents=[out], help="lossless DCZ compression (byte-exact restore)")
    p.add_argument("src")
    p.add_argument("dst", nargs="?")
    p.add_argument("--profile", "-p", choices=["fast", "balanced", "max"], default="balanced")
    p.add_argument("--block-size", help="bytes per independently compressed block, e.g. 8MB")
    p.add_argument("--overwrite", "-f", action="store_true")
    _common(p)
    p.set_defaults(func=cmd_compress)

    p = sub.add_parser("decompress", parents=[out], help="restore a DCZ file")
    p.add_argument("src")
    p.add_argument("dst", nargs="?")
    p.add_argument("--overwrite", "-f", action="store_true")
    _common(p)
    p.set_defaults(func=cmd_decompress)

    p = sub.add_parser("split", parents=[out], help="split a file by rows or size")
    p.add_argument("src")
    p.add_argument("out_dir")
    p.add_argument("--rows", type=int, help="rows per part")
    p.add_argument("--size", help="target size per part, e.g. 100MB (a bare number means MB)")
    p.add_argument("--to", "-t", help="format of the parts (default: same as the input)")
    p.add_argument("--pattern", default="part-{n:05}", help="part name, {n} is the part number (default: part-{n:05})")
    p.add_argument("--extension", help="part file extension (default: from the format)")
    p.add_argument("--profile", "-p", choices=["fast", "balanced", "max"], default="balanced")
    p.add_argument("--compression", "-c")
    p.add_argument("--level", type=int)
    p.add_argument("--overwrite", "-f", action="store_true")
    _reading(p)
    _common(p)
    p.set_defaults(func=cmd_split)

    p = sub.add_parser("merge", parents=[out], help="concatenate files, unifying schemas")
    p.add_argument("sources", nargs="+")
    p.add_argument("-o", "--output", required=True)
    p.add_argument("--to", "-t")
    p.add_argument("--profile", "-p", choices=["fast", "balanced", "max"], default="balanced")
    p.add_argument("--compression", "-c")
    p.add_argument("--level", type=int)
    p.add_argument("--types", choices=["infer", "string", "loose"], default="infer")
    p.add_argument("--schema")
    p.add_argument("--overwrite", "-f", action="store_true")
    p.set_defaults(func=cmd_merge)

    for name, func, helptext in (
        ("info", cmd_info, "describe a file (format, rows, schema, codecs)"),
        ("head", cmd_head, "show the first rows"),
        ("count", cmd_count, "count rows exactly"),
        ("schema", cmd_schema, "show the inferred schema"),
        ("sniff", cmd_sniff, "show the detected CSV dialect"),
        ("stats", cmd_stats, "profile every column"),
    ):
        p = sub.add_parser(name, parents=[out], help=helptext)
        p.add_argument("file")
        if name == "info":
            p.add_argument("--exact", action="store_true", help="count rows exactly (scans text files)")
        if name == "head":
            p.add_argument("-n", type=int, default=10, help="rows to show (default: 10)")
        if name == "stats":
            p.add_argument("--top", type=int, default=5, help="most common values to list")
        _reading(p)
        _common(p)
        p.set_defaults(func=func)

    p = sub.add_parser("verify", parents=[out], help="check that two files hold the same data")
    p.add_argument("a")
    p.add_argument("b")
    p.set_defaults(func=cmd_verify)

    p = sub.add_parser("bench", parents=[out], help="compare formats and codecs on your data")
    p.add_argument("src")
    p.add_argument("--quick", action="store_true", help="only the most useful candidates")
    p.add_argument("--no-read", action="store_true", help="skip read-back timing")
    p.set_defaults(func=cmd_bench)

    p = sub.add_parser("doctor", parents=[out], help="show resources, engines and codecs")
    p.set_defaults(func=cmd_doctor)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 2
    if getattr(args, "progress", None) is None and not (getattr(args, "quiet", False) or getattr(args, "json", False)):
        args.progress = sys.stderr.isatty() or None
    if getattr(args, "quiet", False) or getattr(args, "json", False):
        args.progress = None
    try:
        return int(args.func(args) or 0)
    except KeyboardInterrupt:
        print("\ninterrupted (no partial output was left behind)", file=sys.stderr)
        return 130
    except (DataconvError, OSError, ValueError) as exc:
        print(f"dataconv: error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
