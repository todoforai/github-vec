#!/usr/bin/env python3
import argparse
import asyncio
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import aiohttp
from config import READMES_DIR
from db import (
    count_github_origins, count_recent_github_origins,
    iter_github_origins_batched, iter_recent_github_origins_batched,
)

MIN_SIZE = 500
MAX_CHARS = 50000
CONCURRENCY = 10
MAX_RETRIES = 3

BRANCHES = ["main", "master"]
# Ordered by frequency (top 12 cover 99%+ of repos)
README_NAMES = [
    "README.md",       # 33593
    "readme.md",       # 1047
    "README.rst",      # 721
    "README",          # 587
    "Readme.md",       # 359
    "README.markdown", # 222
    "README.txt",      # 181
    "README.adoc",     # 111
    "readme.txt",      # 108
    "README.MD",       # 106
    "README.rdoc",     # 96
    "ReadMe.md",       # 69
    # Uncomment for broader coverage (~1% more):
    # "readme.rst", "README.textile", "README.org", "readme.html",
    # "README.mdown", "ReadMe.txt", "README.mkd", "README.asciidoc",
    # "readme.markdown", "Readme.markdown", "README.mkdn",
    # "readme.asciidoc", "readme.adoc", "Readme.txt", "README.TXT",
]


@dataclass
class FetchResult:
    content: str | None = None
    branch: str = ""
    filename: str = ""
    status: int = 0  # last non-404 status, or 404 if all were 404


@dataclass
class Stats:
    success: int = 0
    errors: dict[int, int] = None  # status code -> count
    skipped: int = 0
    too_small: int = 0
    truncated: int = 0
    processed: int = 0
    created_dirs: set[str] = None  # track created directories

    def __post_init__(self):
        if self.errors is None:
            self.errors = {}
        if self.created_dirs is None:
            self.created_dirs = set()


class ProxyPool:
    def __init__(self, proxy_file: str | None = None):
        self.proxies: list[str] = []
        if proxy_file:
            self._load(Path(proxy_file))

    def _load(self, path: Path):
        if not path.exists():
            return
        for line in path.read_text().strip().split("\n"):
            parts = line.strip().split(":")
            if len(parts) == 4:
                ip, port, user, pwd = parts
                self.proxies.append(f"http://{user}:{pwd}@{ip}:{port}")
            elif len(parts) == 2:
                ip, port = parts
                self.proxies.append(f"http://{ip}:{port}")
        if self.proxies:
            print(f"Loaded {len(self.proxies)} proxies")

    def get(self) -> str | None:
        return random.choice(self.proxies) if self.proxies else None


async def fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    proxy: str | None,
    repo: str,
    headers: dict | None = None,
) -> tuple[aiohttp.ClientResponse | None, str | None]:
    """Fetch URL with retry on 429 and transient errors. Returns (response, error_msg)."""
    last_err = None
    for retry in range(MAX_RETRIES):
        try:
            resp = await session.get(url, proxy=proxy, headers=headers)
            if resp.status in (429, 500, 502, 503, 504):
                wait = 2 ** retry
                print(f"\n[WARN] {repo}: {resp.status} error, retry {retry+1}/{MAX_RETRIES} in {wait}s")
                await asyncio.sleep(wait)
                continue
            return resp, None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_err = f"{type(e).__name__}: {e}"
            if retry < MAX_RETRIES - 1:
                wait = 2 ** retry
                await asyncio.sleep(wait)
                continue
            return None, last_err
        except Exception as e:
            return None, f"{type(e).__name__}: {e}"
    return None, last_err or "max retries exceeded"


async def try_raw_fetch(
    session: aiohttp.ClientSession,
    repo: str,
    proxy_pool: ProxyPool,
) -> FetchResult:
    """Try fetching README from raw.githubusercontent.com."""
    proxy = proxy_pool.get()  # Get proxy once per repo, like TS version
    last_status = 404  # default if all are 404
    for readme in README_NAMES:
        for branch in BRANCHES:
            url = f"https://raw.githubusercontent.com/{repo}/{branch}/{readme}"
            resp, err = await fetch_with_retry(session, url, proxy, repo)

            if err:
                print(f"\n[ERR] {repo}: {err}")
                last_status = 0  # connection/timeout error
                continue

            try:
                if resp.status == 200:
                    content = await resp.text(errors="replace")
                    return FetchResult(content=content, branch=branch, filename=readme)
                elif resp.status == 451:
                    return FetchResult(status=451)  # DMCA - return immediately
                elif resp.status != 404:
                    print(f"\n[WARN] {repo}: raw.githubusercontent returned {resp.status} for {branch}/{readme}")
                    last_status = resp.status
            finally:
                resp.close()

    return FetchResult(status=last_status)



async def fetch_and_save(
    session: aiohttp.ClientSession,
    url: str,
    output_dir: Path,
    errors_dir: Path,
    proxy_pool: ProxyPool,
    stats: Stats,
    total: int,
    start_time: float,
    existing_repos: set[str],
    error_repos: set[str],
):
    match = re.search(r"github\.com/([^/]+/[^/]+)", url)
    if not match:
        stats.errors[0] = stats.errors.get(0, 0) + 1
        return

    repo = match.group(1).removesuffix(".git")
    repo_file = repo.replace("/", "_")

    # Skip if already exists or has error (O(1) set lookup)
    if repo_file in existing_repos or repo_file in error_repos:
        stats.skipped += 1
        stats.processed += 1
        return

    # Try raw fetch only
    result = await try_raw_fetch(session, repo, proxy_pool)

    stats.processed += 1

    if not result.content:
        # Save to .errors/{status}/
        status_key = str(result.status)
        if status_key not in stats.created_dirs:
            (errors_dir / status_key).mkdir(exist_ok=True)
            stats.created_dirs.add(status_key)
        (errors_dir / status_key / repo_file).touch()
        stats.errors[result.status] = stats.errors.get(result.status, 0) + 1
        return

    if len(result.content) < MIN_SIZE:
        # Save to .errors/tooSmall/ so we don't retry
        too_small_dir = errors_dir / "tooSmall"
        if "tooSmall" not in stats.created_dirs:
            too_small_dir.mkdir(exist_ok=True)
            stats.created_dirs.add("tooSmall")
        (too_small_dir / repo_file).touch()
        stats.too_small += 1
        return

    if len(result.content) > MAX_CHARS:
        result.content = result.content[:MAX_CHARS] + "\n\n[TRUNCATED]"
        stats.truncated += 1

    out_file = f"{repo_file}_{result.branch}_{result.filename}"
    (output_dir / out_file).write_text(result.content)
    stats.success += 1

    if stats.processed % 100 == 0:
        elapsed = time.time() - start_time
        completed = stats.success + stats.too_small + sum(stats.errors.values())
        rate = completed / elapsed if elapsed > 0 else 0
        error_total = sum(stats.errors.values())
        print(f"\r[{stats.processed}/{total}] ✓ {stats.success} ✗ {error_total} ({rate:.0f}/s)", end="", flush=True)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="Limit number of repos to fetch")
    parser.add_argument("--offset", type=int, default=0, help="Offset to start from")
    parser.add_argument("--full", action="store_true", help="Use full dataset instead of 6k popular repos")
    parser.add_argument("--min-date", type=str, help="Only fetch repos with last visit >= this date (YYYY-MM-DD)")
    parser.add_argument("--proxies", type=str, help="Path to proxy list file (ip:port:user:pass per line)")
    args = parser.parse_args()

    proxy_pool = ProxyPool(args.proxies)

    output_dir = Path(READMES_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    errors_dir = output_dir / ".errors"
    errors_dir.mkdir(exist_ok=True)

    # Get total count (fast with DuckDB)
    if args.min_date:
        min_date = datetime.strptime(args.min_date, "%Y-%m-%d")
        total_count = count_recent_github_origins(min_date=min_date, full=args.full)
        print(f"Total origins >= {args.min_date}: {total_count:,}")
    else:
        min_date = None
        total_count = count_github_origins(full=args.full)
        print(f"Total GitHub origins: {total_count:,}")

    # Calculate effective total after offset/limit
    effective_total = total_count - args.offset
    if args.limit:
        effective_total = min(args.limit, effective_total)

    print(f"Fetching {effective_total:,} repos (concurrency: {CONCURRENCY})...")
    if args.offset:
        print(f"Starting at offset: {args.offset:,}")

    # Pre-scan existing files for O(1) lookup (avoid slow glob per repo)
    print("Scanning existing files...")
    existing_repos: set[str] = set()
    for f in output_dir.iterdir():
        if f.is_file():
            # filename format: owner_repo_branch_readme -> extract owner_repo
            parts = f.name.rsplit("_", 2)
            if len(parts) >= 3:
                existing_repos.add(parts[0])
    # Scan all error subdirectories
    error_repos: set[str] = set()
    if errors_dir.exists():
        for status_dir in errors_dir.iterdir():
            if status_dir.is_dir():
                for f in status_dir.iterdir():
                    error_repos.add(f.name)
    print(f"  {len(existing_repos):,} existing, {len(error_repos):,} errors")
    print()

    stats = Stats()
    start_time = time.time()
    total = effective_total
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    timeout = aiohttp.ClientTimeout(total=30)

    # Get batched iterator (memory-efficient)
    if min_date:
        origin_batches = iter_recent_github_origins_batched(
            min_date=min_date, limit=args.limit, offset=args.offset, full=args.full
        )
    else:
        origin_batches = iter_github_origins_batched(
            limit=args.limit, offset=args.offset, full=args.full
        )

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Bounded task pool - process URLs immediately as they load
        in_flight: set[asyncio.Task] = set()
        batch_offset = args.offset

        for batch in origin_batches:
            print(f"\rLoading batch at offset {batch_offset:,}...", end="", flush=True)

            for url in batch:
                # Wait if at capacity
                while len(in_flight) >= CONCURRENCY:
                    done, in_flight_new = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
                    in_flight = in_flight_new

                # Start task immediately
                task = asyncio.create_task(
                    fetch_and_save(session, url, output_dir, errors_dir, proxy_pool, stats, total, start_time, existing_repos, error_repos)
                )
                in_flight.add(task)

            batch_offset += len(batch)

        # Wait for remaining tasks
        if in_flight:
            await asyncio.wait(in_flight)

    print(f"\n\nDone! {stats.success} READMEs saved to {output_dir}/")
    print(f"{stats.skipped} skipped, {stats.too_small} too small, {stats.truncated} truncated")
    if stats.errors:
        print("Errors by status code:")
        for status, count in sorted(stats.errors.items()):
            label = {0: "timeout/connection", 404: "not found", 451: "DMCA"}.get(status, str(status))
            print(f"  {status} ({label}): {count:,}")


if __name__ == "__main__":
    asyncio.run(main())
