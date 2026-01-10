# Software Heritage Dataset

Public S3 bucket: `s3://softwareheritage` (no credentials needed, use `--no-sign-request`)

## Available Snapshots

| Snapshot | Type | Size |
|----------|------|------|
| 2025-11-28 | Full | ~30 TiB |
| 2025-10-08 | Full | 30 TiB |
| 2025-10-08-history-hosting | Metadata only | 1.7 TiB |
| 2025-05-18-popular-1k | Top 1k starred repos | ~63 KB origins |
| 2023-09-06-popular-6k | Top 6k starred repos | ~253 KB origins |

## Dataset Formats

### Columnar (ORC)
- Location: `s3://softwareheritage/graph/{snapshot}/orc/`
- Format: Apache ORC (Spark/Trino/DuckDB compatible)
- Use: Query with SQL, extract specific tables

### Compressed Graph
- Location: `s3://softwareheritage/graph/{snapshot}/compressed/`
- Format: WebGraph compressed format
- Use: Graph traversal algorithms

## Tables & Schema

### origin
Repository URLs.
| Column | Type |
|--------|------|
| id | string |
| url | string |

### revision
Git commits.
| Column | Type |
|--------|------|
| id | string (commit hash) |
| message | string |
| author | string (anonymized) |
| committer | string (anonymized) |
| date | timestamp |
| date_offset | int |
| committer_date | timestamp |
| directory | string (root tree ref) |

### content
Archived file metadata (not content itself).
| Column | Type |
|--------|------|
| sha1 | string |
| sha1_git | string |
| sha256 | string |
| blake2s256 | string |
| length | int |
| status | string |

### origin_visit
Crawl records.
| Column | Type |
|--------|------|
| origin | string |
| visit | int |
| date | timestamp |
| type | string (git/svn/hg) |

### origin_visit_status
Visit outcomes.
| Column | Type |
|--------|------|
| origin | string |
| visit | int |
| snapshot_id | string |
| status | string |

### directory
Project directories.
| Column | Type |
|--------|------|
| id | string (SHA-1) |

### directory_entry
Files within directories.
| Column | Type |
|--------|------|
| directory_id | string |
| name | string |
| type | string (rev/dir/file) |
| target | string |
| perms | int |

### release
Tagged versions.
| Column | Type |
|--------|------|
| id | string |
| target | string |
| name | string |
| message | string |
| date | timestamp |
| author | string |

### snapshot & snapshot_branch
Branch snapshots and named references.

## History-Hosting vs Full

| Dataset | Includes | Size |
|---------|----------|------|
| Full | All files, all commits, all repos | 30 TiB |
| History-Hosting | Commits, tags, branches, root dirs only | 1.7 TiB |

History-hosting excludes file contents - useful for commit analysis, not for code search.

## Estimated Scale

| Metric | Value |
|--------|-------|
| Origin URLs (full) | ~378 million |
| GitHub repos (subset) | ~100-200 million |
| Origin table size | 15.9 GB |

## Usage Examples

### List bucket
```bash
aws s3 ls s3://softwareheritage/graph/ --no-sign-request
```

### Download origins (popular-6k)
```bash
aws s3 sync s3://softwareheritage/graph/2023-09-06-popular-6k/orc/origin/ ./origins/ --no-sign-request
```

### Extract GitHub URLs with Python
```python
import pyarrow.orc as orc
import os

urls = []
for f in os.listdir('./origins'):
    table = orc.read_table(f'./origins/{f}')
    for url in table['url'].to_pylist():
        if url.startswith('https://github.com/'):
            urls.append(url)
```

## Our Pipeline

1. Download origins from popular-6k subset
2. Filter to GitHub URLs only (5,577 repos)
3. Fetch READMEs via `raw.githubusercontent.com` (no rate limit)
4. Fallback to GitHub API for edge cases
5. Filter: drop <500 bytes, truncate >50k chars
6. Embed with Qwen3-Embedding-8B
7. Store in Qdrant

### Stats
- READMEs fetched: 5,758
- After filtering: 5,751
- Embedding cost: ~$0.22
- Total size: 70 MB
