"""Legacy entry point, kept for existing scripts:
``python csv_splitter_converter.py large.csv out_dir --chunk-size 100``.

New code should use ``dataconv split`` / ``dataconv.split``, which hits the target
output size (measured, not estimated) and never drops malformed rows silently.
"""

import argparse
import sys
from pathlib import Path

try:
    import dataconv
except ImportError:  # running from a source checkout without installing
    sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
    import dataconv


class CSVSplitterConverter:
    def __init__(self, chunk_size_mb=250):
        """chunk_size_mb: target size of each Parquet chunk in megabytes."""
        self.chunk_size_mb = float(chunk_size_mb)
        self.bytes_per_mb = 1024 * 1024

    def split_and_convert(self, input_csv, output_dir, chunk_size_mb=None):
        """Split a CSV into Parquet chunks named chunk_0000.parquet, chunk_0001.parquet, ...

        Returns the list of created file paths.
        """
        if chunk_size_mb is not None:
            self.chunk_size_mb = float(chunk_size_mb)
        target = int(self.chunk_size_mb * self.bytes_per_mb)
        report = dataconv.split(input_csv, output_dir, size=max(1, target), to="parquet",
                                pattern="chunk_{n:04}", overwrite=True, progress=sys.stderr.isatty())
        created = [Path(p["path"]) for p in report.parts]
        print(f"\nTotal rows: {report.rows:,}")
        print(f"Created {len(created)} files:")
        for p in report.parts:
            print(f"- {Path(p['path']).name}: {p['bytes'] / self.bytes_per_mb:.2f} MB")
        for w in report.warnings:
            print(f"note: {w}")
        return created


def main():
    parser = argparse.ArgumentParser(description="Split large CSV file and convert to Parquet")
    parser.add_argument("input_csv", help="Path to input CSV file")
    parser.add_argument("output_dir", help="Directory to save output files")
    parser.add_argument("--chunk-size", type=float, default=250.0, help="Target size for each chunk in megabytes (default: 250.0)")
    args = parser.parse_args()
    CSVSplitterConverter(chunk_size_mb=args.chunk_size).split_and_convert(args.input_csv, args.output_dir)


if __name__ == "__main__":
    main()
