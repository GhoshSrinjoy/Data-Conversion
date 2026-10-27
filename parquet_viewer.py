"""Legacy entry point, kept for existing scripts: ``python parquet_viewer.py file.parquet``.

New code should use ``dataconv info`` / ``dataconv head`` (any format, no full load).
"""

import argparse
import sys
from pathlib import Path

try:
    import dataconv
except ImportError:  # running from a source checkout without installing
    sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
    import dataconv

from dataconv.cli import _table


def view_parquet(file_path, num_rows=5, show_info=True, show_schema=True):
    """Print a summary of a Parquet (or any supported) file and return its data.

    Returns a pandas DataFrame when pandas is installed (as before), otherwise a
    pyarrow Table, or None when the file cannot be read.
    """
    try:
        info = dataconv.info(file_path)
        print(f"\n{'=' * 50}")
        print(f"File: {file_path}")
        print(f"{'=' * 50}")
        if show_info:
            print("\nDataFrame Info:")
            print(f"Number of rows: {info.get('rows')}")
            print(f"Number of columns: {len(info.get('columns', []))}")
            print(f"File size: {info.get('file_size')} bytes")
        if show_schema:
            print("\nSchema:")
            for c in info.get("columns", []):
                print(f"{c['name']}: {c.get('type') or c.get('logical_type') or c.get('physical_type')}")
        preview = dataconv.head(file_path, num_rows)
        print(f"\nFirst {num_rows} rows:")
        print(_table(preview["columns"], preview["rows"]))
        try:
            table = dataconv.load(file_path)
        except dataconv.MissingDependencyError:
            return None
        try:
            return table.to_pandas()
        except ImportError:
            return table
    except Exception as e:
        print(f"Error reading parquet file: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="View Parquet file contents")
    parser.add_argument("file", help="Path to Parquet file")
    parser.add_argument("--rows", type=int, default=5, help="Number of rows to display (default: 5)")
    parser.add_argument("--no-info", action="store_true", help="Skip showing DataFrame info")
    parser.add_argument("--no-schema", action="store_true", help="Skip showing schema")
    args = parser.parse_args()
    if not Path(args.file).exists():
        print(f"Error: File {args.file} does not exist")
        return
    view_parquet(args.file, args.rows, not args.no_info, not args.no_schema)


if __name__ == "__main__":
    main()
