from pathlib import Path
from datetime import datetime
from typing import Iterator, Generator
import pyarrow.parquet as pq
import pyarrow.compute as pc
import duckdb
from config import (
    ORIGINS_6K_PARQUET, ORIGINS_FULL_PARQUET,
    VISITS_6K_PARQUET, VISITS_FULL_PARQUET,
)


def count_github_origins(full: bool = False) -> int:
    """Count total GitHub origins in parquet file."""
    parquet_file = ORIGINS_FULL_PARQUET if full else ORIGINS_6K_PARQUET
    meta = pq.read_metadata(parquet_file)
    return meta.num_rows


def get_github_origins(
    limit: int | None = None,
    offset: int = 0,
    full: bool = False
) -> list[str]:
    """Get GitHub origin URLs from parquet file."""
    parquet_file = ORIGINS_FULL_PARQUET if full else ORIGINS_6K_PARQUET
    table = pq.read_table(parquet_file, columns=["url"])
    urls = table["url"].to_pylist()

    if offset:
        urls = urls[offset:]
    if limit:
        urls = urls[:limit]

    return urls


def get_recent_github_origins(
    min_date: datetime,
    limit: int | None = None,
    offset: int = 0,
    full: bool = False,
    # Legacy params (ignored, kept for compatibility)
    origins_dir: str | None = None,
    visits_dir: str | None = None,
) -> list[str]:
    """Get GitHub origin URLs filtered by minimum last visit date."""
    visits_parquet = VISITS_FULL_PARQUET if full else VISITS_6K_PARQUET

    print(f"Loading visits from {Path(visits_parquet).name}...")
    table = pq.read_table(visits_parquet)
    print(f"  {table.num_rows:,} total origins")

    print(f"Filtering to visits >= {min_date.strftime('%Y-%m-%d')}...")
    mask = pc.greater_equal(table["date"], min_date)
    filtered = table.filter(mask)
    count = filtered.num_rows
    print(f"  {count:,} recent origins")

    # Apply offset/limit at Arrow level (before conversion)
    if offset:
        filtered = filtered.slice(offset)
    if limit:
        filtered = filtered.slice(0, limit)

    print(f"Converting {filtered.num_rows:,} URLs to Python list (this may take a moment)...")
    urls = filtered["origin"].to_pylist()
    print(f"  Done.")

    return urls


def iter_github_origins_batched(
    limit: int | None = None,
    offset: int = 0,
    full: bool = False,
    batch_size: int = 50000,
) -> Generator[list[str], None, None]:
    """Yield GitHub origin URLs in batches using DuckDB (memory-efficient)."""
    parquet_file = ORIGINS_FULL_PARQUET if full else ORIGINS_6K_PARQUET
    db = duckdb.connect(":memory:")

    current_offset = offset
    remaining = limit

    while True:
        batch_limit = batch_size if remaining is None else min(batch_size, remaining)
        query = f"SELECT url FROM '{parquet_file}' LIMIT {batch_limit} OFFSET {current_offset}"
        result = db.execute(query).fetchall()

        if not result:
            break

        urls = [row[0] for row in result]
        yield urls

        current_offset += len(urls)
        if remaining is not None:
            remaining -= len(urls)
            if remaining <= 0:
                break

    db.close()


def iter_recent_github_origins_batched(
    min_date: datetime,
    limit: int | None = None,
    offset: int = 0,
    full: bool = False,
    batch_size: int = 50000,
) -> Generator[list[str], None, None]:
    """Yield recent GitHub origin URLs in batches using DuckDB (memory-efficient)."""
    visits_parquet = VISITS_FULL_PARQUET if full else VISITS_6K_PARQUET
    db = duckdb.connect(":memory:")

    min_date_str = min_date.strftime("%Y-%m-%d")
    current_offset = offset
    remaining = limit

    while True:
        batch_limit = batch_size if remaining is None else min(batch_size, remaining)
        query = f"""
            SELECT origin FROM '{visits_parquet}'
            WHERE date >= '{min_date_str}'
            LIMIT {batch_limit} OFFSET {current_offset}
        """
        result = db.execute(query).fetchall()

        if not result:
            break

        urls = [row[0] for row in result]
        yield urls

        current_offset += len(urls)
        if remaining is not None:
            remaining -= len(urls)
            if remaining <= 0:
                break

    db.close()


def count_recent_github_origins(min_date: datetime, full: bool = False) -> int:
    """Count recent GitHub origins using DuckDB (fast)."""
    visits_parquet = VISITS_FULL_PARQUET if full else VISITS_6K_PARQUET
    min_date_str = min_date.strftime("%Y-%m-%d")
    db = duckdb.connect(":memory:")
    result = db.execute(f"SELECT COUNT(*) FROM '{visits_parquet}' WHERE date >= '{min_date_str}'").fetchone()
    db.close()
    return result[0]


if __name__ == "__main__":
    # Quick test
    count = count_github_origins(full=True)
    print(f"GitHub origins: {count:,}")

    sample = get_github_origins(limit=5, full=True)
    print("Sample origins:")
    for url in sample:
        print(f"  {url}")

    print()
    recent = get_recent_github_origins(
        min_date=datetime(2023, 1, 1),
        limit=5,
        full=True
    )
    print("Sample recent:")
    for url in recent:
        print(f"  {url}")
