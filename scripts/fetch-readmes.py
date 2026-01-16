#!/usr/bin/env python3
import argparse
import asyncio
import base64
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import aiohttp
from config import READMES_DIR
from db import get_github_origins, get_recent_github_origins, count_github_origins

MIN_SIZE = 500
MAX_CHARS = 50000
CONCURRENCY = 400
MAX_RETRIES = 3

BRANCHES = ["main", "master"]
README_NAMES = [
    "README.md", "readme.md", "Readme.md", "ReadMe.md",
    "README.markdown", "readme.markdown", "Readme.markdown",
    "README.mkd", "README.mdown", "README.mkdn",
    "README.asciidoc", "readme.asciidoc", "README.adoc", "readme.adoc",
    "README.rst", "readme.rst",
    "README.rdoc",
    "README.textile",
    "README.org",
    "README.txt", "Readme.txt", "readme.txt", "README.TXT",
    "README.MD",
    "readme.html",
    "README"
]


@dataclass
class FetchResult:
    content: str | None = None
    branch: str = ""
    filename: str = ""
    used_api: bool = False


@dataclass
class Stats:
    success: int = 0
    failed: int = 0
    skipped: int = 0
    too_small: int = 0
    truncated: int = 0
    api_fallbacks: int = 0
    processed: int = 0


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
    proxy_pool: ProxyPool,
    repo: str,
    headers: dict | None = None,
) -> tuple[aiohttp.ClientResponse | None, str | None]:
    """Fetch URL with retry on 429 and transient errors. Returns (response, error_msg)."""
    last_err = None
    for retry in range(MAX_RETRIES):
        proxy = proxy_pool.get()
        try:
            resp = await session.get(url, proxy=proxy, headers=headers)
            if resp.status == 429:
                wait = 2 ** retry
                print(f"\n[WARN] {repo}: 429 rate limited, retry {retry+1}/{MAX_RETRIES} in {wait}s")
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
    for readme in README_NAMES:
        for branch in BRANCHES:
            url = f"https://raw.githubusercontent.com/{repo}/{branch}/{readme}"
            resp, err = await fetch_with_retry(session, url, proxy_pool, repo)

            if err:
                print(f"\n[ERR] {repo}: {err}")
                continue

            try:
                if resp.status == 200:
                    content = await resp.text(errors="replace")
                    return FetchResult(content=content, branch=branch, filename=readme)
                elif resp.status != 404:
                    print(f"\n[WARN] {repo}: raw.githubusercontent returned {resp.status} for {branch}/{readme}")
            finally:
                resp.close()

    return FetchResult()


async def try_api_fetch(
    session: aiohttp.ClientSession,
    repo: str,
    proxy_pool: ProxyPool,
    token: str | None,
) -> FetchResult:
    """Try fetching README from GitHub API."""
    url = f"https://api.github.com/repos/{repo}/readme"
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    resp, err = await fetch_with_retry(session, url, proxy_pool, repo, headers)

    if err:
        print(f"\n[ERR] {repo}: API fallback {err}")
        return FetchResult()

    try:
        if resp.status == 200:
            data = await resp.json()
            content = base64.b64decode(data["content"]).decode("utf-8", errors="replace")
            return FetchResult(content=content, branch="default", filename=data["name"], used_api=True)
        elif resp.status != 404:
            print(f"\n[WARN] {repo}: GitHub API returned {resp.status}")
    finally:
        resp.close()

    return FetchResult()


async def fetch_and_save(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
    output_dir: Path,
    failed_dir: Path,
    proxy_pool: ProxyPool,
    token: str | None,
    stats: Stats,
    total: int,
    start_time: float,
    existing_repos: set[str],
    failed_repos: set[str],
):
    async with semaphore:
        match = re.search(r"github\.com/([^/]+/[^/]+)", url)
        if not match:
            stats.failed += 1
            return

        repo = match.group(1).removesuffix(".git")
        repo_file = repo.replace("/", "_")

        # Skip if known failed or already exists (O(1) set lookup)
        if repo_file in failed_repos or repo_file in existing_repos:
            stats.skipped += 1
            stats.processed += 1
            return

        # Try raw fetch, then API fallback
        result = await try_raw_fetch(session, repo, proxy_pool)
        if not result.content:
            result = await try_api_fetch(session, repo, proxy_pool, token)
            if result.used_api:
                stats.api_fallbacks += 1

        stats.processed += 1

        if not result.content:
            stats.failed += 1
            (failed_dir / repo_file).touch()
            return

        if len(result.content) < MIN_SIZE:
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
            rate = stats.processed / elapsed if elapsed > 0 else 0
            print(f"\r[{stats.processed}/{total}] ✓ {stats.success} ✗ {stats.failed} ({rate:.0f}/s)", end="", flush=True)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="Limit number of repos to fetch")
    parser.add_argument("--offset", type=int, default=0, help="Offset to start from")
    parser.add_argument("--full", action="store_true", help="Use full dataset instead of 6k popular repos")
    parser.add_argument("--min-date", type=str, help="Only fetch repos with last visit >= this date (YYYY-MM-DD)")
    parser.add_argument("--proxies", type=str, help="Path to proxy list file (ip:port:user:pass per line)")
    args = parser.parse_args()

    proxy_pool = ProxyPool(args.proxies)
    token = os.environ.get("GITHUB_TOKEN")

    output_dir = Path(READMES_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    failed_dir = output_dir / ".failed"
    failed_dir.mkdir(exist_ok=True)

    # Load origins
    if args.min_date:
        min_date = datetime.strptime(args.min_date, "%Y-%m-%d")
        origins = get_recent_github_origins(min_date=min_date, limit=args.limit, offset=args.offset, full=args.full)
    else:
        total_count = count_github_origins(full=args.full)
        print(f"Total GitHub origins: {total_count:,}")
        origins = get_github_origins(limit=args.limit, offset=args.offset, full=args.full)

    print(f"Fetching {len(origins):,} repos (concurrency: {CONCURRENCY})...")
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
    failed_repos = {f.name for f in failed_dir.iterdir()}
    print(f"  {len(existing_repos):,} existing, {len(failed_repos):,} failed")
    print()

    stats = Stats()
    start_time = time.time()
    semaphore = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    timeout = aiohttp.ClientTimeout(total=30)

    BATCH_SIZE = min(10000, len(origins) or 1)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        for i in range(0, len(origins), BATCH_SIZE):
            batch = origins[i:i + BATCH_SIZE]
            tasks = [
                fetch_and_save(session, url, semaphore, output_dir, failed_dir, proxy_pool, token, stats, len(origins), start_time, existing_repos, failed_repos)
                for url in batch
            ]
            await asyncio.gather(*tasks)

    print(f"\n\nDone! {stats.success} READMEs saved to {output_dir}/")
    print(f"{stats.skipped} skipped, {stats.too_small} too small, {stats.truncated} truncated")
    if stats.api_fallbacks > 0:
        print(f"{stats.api_fallbacks} used API fallback")
    if stats.failed > 0:
        print(f"{stats.failed} repos had no README")


if __name__ == "__main__":
    asyncio.run(main())
