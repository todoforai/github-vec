#!/usr/bin/env python3
"""
One-time conversion: ORC files → single Parquet file with just GitHub origins.

This makes subsequent queries much faster since:
- Single file instead of 128
- Pre-filtered to GitHub only
- Parquet is optimized for this kind of analytical query
"""
import argparse
import pyarrow as pa
import pyarrow.orc as orc
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pathlib import Path
from config import ORIGINS_FULL_DIR, ORIGIN_VISIT_FULL_DIR, ORIGINS_6K_DIR, ORIGIN_VISIT_6K_DIR

parser = argparse.ArgumentParser()
parser.add_argument("--full", action="store_true", help="Use full dataset instead of 6k")
parser.add_argument("--output-dir", type=str, default="/home/root/data", help="Output directory")
parser.add_argument("--visits-only", action="store_true", help="Only convert visits, skip origins")
args = parser.parse_args()

output_dir = Path(args.output_dir)
output_dir.mkdir(parents=True, exist_ok=True)

suffix = "full" if args.full else "6k"
origins_dir = ORIGINS_FULL_DIR if args.full else ORIGINS_6K_DIR
visits_dir = ORIGIN_VISIT_FULL_DIR if args.full else ORIGIN_VISIT_6K_DIR


def get_orc_files(dir_path: str) -> list[Path]:
    orc_dir = Path(dir_path)
    if not orc_dir.exists():
        return []
    return sorted(orc_dir.iterdir())


def convert_origins():
    """Convert origins ORC files to single Parquet with just GitHub URLs."""
    orc_files = get_orc_files(origins_dir)
    print(f"Converting {len(orc_files)} origin files to Parquet...")

    output_file = output_dir / f"github_origins_{suffix}.parquet"

    tables = []
    total_github = 0

    for i, orc_file in enumerate(orc_files):
        table = orc.read_table(str(orc_file), columns=["url"])
        mask = pc.starts_with(table["url"], "https://github.com/")
        filtered = table.filter(mask)

        if filtered.num_rows > 0:
            tables.append(filtered)
            total_github += filtered.num_rows

        if (i + 1) % 10 == 0 or (i + 1) == len(orc_files):
            print(f"  [{i + 1}/{len(orc_files)}] {total_github:,} GitHub origins")

    print(f"Concatenating {len(tables)} tables...")
    combined = pa.concat_tables(tables)

    print(f"Writing {combined.num_rows:,} rows to {output_file}...")
    pq.write_table(combined, output_file, compression="zstd")

    size_mb = output_file.stat().st_size / 1024 / 1024
    print(f"Done: {output_file} ({size_mb:.1f} MB)")


def convert_visits():
    """Convert origin_visit ORC files to single Parquet with just GitHub URLs.

    Deduplicates to keep only the most recent visit per origin.
    Uses streaming deduplication to avoid loading all data into memory.
    """
    orc_files = get_orc_files(visits_dir)
    print(f"\nConverting {len(orc_files)} visit files to Parquet...")

    output_file = output_dir / f"github_visits_{suffix}.parquet"

    # Stream and deduplicate: dict of origin -> max_date
    latest_visits: dict[str, object] = {}
    total_github = 0

    for i, orc_file in enumerate(orc_files):
        table = orc.read_table(str(orc_file), columns=["origin", "date"])
        mask = pc.starts_with(table["origin"], "https://github.com/")
        filtered = table.filter(mask)

        if filtered.num_rows > 0:
            origins = filtered["origin"].to_pylist()
            dates = filtered["date"].to_pylist()
            total_github += len(origins)

            for origin, date in zip(origins, dates):
                if origin not in latest_visits or date > latest_visits[origin]:
                    latest_visits[origin] = date

        if (i + 1) % 10 == 0 or (i + 1) == len(orc_files):
            print(f"  [{i + 1}/{len(orc_files)}] {total_github:,} visits processed, {len(latest_visits):,} unique origins")

    print(f"Building table from {len(latest_visits):,} unique origins...")
    combined = pa.table({
        "origin": list(latest_visits.keys()),
        "date": list(latest_visits.values())
    })

    print(f"Writing {combined.num_rows:,} rows to {output_file}...")
    pq.write_table(combined, output_file, compression="zstd")

    size_mb = output_file.stat().st_size / 1024 / 1024
    print(f"Done: {output_file} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    if not args.visits_only:
        convert_origins()
    convert_visits()

    print("\n" + "=" * 50)
    print("Conversion complete!")
    print(f"You can now update db.py to read from:")
    if not args.visits_only:
        print(f"  - {output_dir}/github_origins_{suffix}.parquet")
    print(f"  - {output_dir}/github_visits_{suffix}.parquet")
