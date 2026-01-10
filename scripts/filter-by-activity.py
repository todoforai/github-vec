#!/usr/bin/env python3
"""
Filter Software Heritage origins by recent activity.

Uses origin_visit table to find repos with visits in the last N days.
Note: visit date = when SWH crawled the repo, implies recent commits.

Usage:
    python scripts/filter-by-activity.py --days 365 --output /tmp/active-origins.txt
"""

import argparse
import os
import pandas as pd
import pyarrow.orc as orc
import pyarrow as pa


def download_origin_visit(dataset: str, output_dir: str):
    """Download origin_visit table from S3."""
    import subprocess

    s3_path = f"s3://softwareheritage/graph/{dataset}/orc/origin_visit/"
    os.makedirs(output_dir, exist_ok=True)

    subprocess.run([
        "aws", "s3", "sync", s3_path, output_dir, "--no-sign-request"
    ], check=True)


def load_origin_visit(orc_dir: str) -> pd.DataFrame:
    """Load all origin_visit ORC files into a DataFrame."""
    tables = []
    for f in os.listdir(orc_dir):
        path = os.path.join(orc_dir, f)
        tables.append(orc.read_table(path))

    combined = pa.concat_tables(tables)
    return combined.to_pandas()


def filter_by_activity(df: pd.DataFrame, days: int, reference_date: str = None) -> set:
    """
    Filter origins by recent activity.

    Args:
        df: DataFrame with 'origin' and 'date' columns
        days: Number of days to look back
        reference_date: Reference date (default: max date in data)

    Returns:
        Set of origin URLs with recent activity
    """
    if reference_date:
        ref = pd.Timestamp(reference_date)
    else:
        ref = df['date'].max()

    cutoff = ref - pd.Timedelta(days=days)
    recent = df[df['date'] >= cutoff]

    return set(recent['origin'].unique())


def filter_github_only(origins: set) -> set:
    """Keep only GitHub URLs."""
    return {o for o in origins if o.startswith('https://github.com/')}


def main():
    parser = argparse.ArgumentParser(description='Filter origins by activity')
    parser.add_argument('--dataset', default='2023-09-06-popular-6k',
                        help='SWH dataset name (default: 2023-09-06-popular-6k)')
    parser.add_argument('--days', type=int, default=365,
                        help='Days of activity to consider (default: 365)')
    parser.add_argument('--orc-dir', default='/tmp/origin_visit',
                        help='Directory with origin_visit ORC files')
    parser.add_argument('--download', action='store_true',
                        help='Download ORC files from S3 first')
    parser.add_argument('--github-only', action='store_true',
                        help='Filter to GitHub repos only')
    parser.add_argument('--output', default='/tmp/active-origins.txt',
                        help='Output file for filtered origins')

    args = parser.parse_args()

    # Download if requested
    if args.download:
        print(f"Downloading origin_visit from {args.dataset}...")
        download_origin_visit(args.dataset, args.orc_dir)

    # Load data
    print(f"Loading origin_visit from {args.orc_dir}...")
    df = load_origin_visit(args.orc_dir)
    print(f"  Total visits: {len(df):,}")
    print(f"  Date range: {df['date'].min()} to {df['date'].max()}")

    # Filter by activity
    print(f"Filtering to last {args.days} days...")
    active = filter_by_activity(df, args.days)
    print(f"  Active origins: {len(active):,}")

    # Filter to GitHub if requested
    if args.github_only:
        active = filter_github_only(active)
        print(f"  GitHub only: {len(active):,}")

    # Save results
    with open(args.output, 'w') as f:
        f.write('\n'.join(sorted(active)))
    print(f"Saved to {args.output}")


if __name__ == '__main__':
    main()
