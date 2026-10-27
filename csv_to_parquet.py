"""Legacy entry point, kept for existing scripts: ``python csv_to_parquet.py data.csv``.

New code should use ``dataconv convert`` / ``dataconv.convert`` directly.
"""

import argparse
import os
import sys
from pathlib import Path

try:
    import dataconv
except ImportError:  # running from a source checkout without installing
    sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
    import dataconv


def convert_csv_to_parquet(input_path, output_path=None):
    """Convert a CSV file to Parquet and return the output path."""
    try:
        dst = output_path or str(Path(input_path).with_suffix(".parquet"))
        dataconv.convert(input_path, dst, to="parquet", overwrite=True)
        return dst
    except Exception as e:
        raise Exception(f"Error converting {input_path}: {e}") from e


def batch_convert_csv_to_parquet(input_dir, output_dir=None):
    """Convert every CSV under input_dir (recursively), mirroring the folder tree."""
    report = dataconv.convert_dir(input_dir, output_dir or input_dir, to="parquet", include=["*.csv"], overwrite=True)
    for src, err in report.failed:
        print(f"Failed to convert {src}: {err}")
    converted = [r.destination for r in report.reports]
    for r in report.reports:
        print(f"Successfully converted: {r.source} -> {r.destination}")
    return converted


def main():
    parser = argparse.ArgumentParser(description="Convert CSV files to Parquet format")
    parser.add_argument("input", help="Input CSV file or directory")
    parser.add_argument("--output", help="Output Parquet file or directory (optional)")
    args = parser.parse_args()
    if os.path.isfile(args.input):
        try:
            print(f"Successfully converted: {args.input} -> {convert_csv_to_parquet(args.input, args.output)}")
        except Exception as e:
            print(f"Conversion failed: {e}")
    elif os.path.isdir(args.input):
        converted = batch_convert_csv_to_parquet(args.input, args.output)
        print(f"\nConverted {len(converted)} files")
    else:
        print(f"Error: {args.input} is not a valid file or directory")


if __name__ == "__main__":
    main()
