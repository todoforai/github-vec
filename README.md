# github-vec

Semantic search over 2.3M GitHub READMEs using vector embeddings.

## Setup

```bash
./setup.sh              # Install deps + Qdrant
qdrant                  # Start Qdrant server (in separate terminal)
bun scripts/ingest.ts   # Ingest READMEs into Qdrant
```

Requires `OPENROUTER_API_KEY` env var for embeddings.

## Data

| Property | Value |
|----------|-------|
| Records | 2,342,435 unique READMEs |
| Size | ~4.8 GB |
| Source | BigQuery `bigquery-public-data.github_repos` |

Schema:
```jsonl
{"content_hash": "9d6a7cca...", "repo_name": "owner/repo", "content": "# Title\n..."}
```

| Field | Type | Description |
|-------|------|-------------|
| `content_hash` | string | SHA-1 hash (unique ID) |
| `repo_name` | string | GitHub repo `owner/repo` |
| `content` | string | Raw README.md markdown |

Sample:
```json
{
  "content_hash": "9d6a7cca12ed5fc9831fec6d97fed2e88b1bb884",
  "repo_name": "nyc-squirrels-2015/dbc_pair_mate_v2",
  "content": "# dbc_pair_mate_v2\nThis a verion 2 of the dbc pair mate ported to Rails.\n"
}
```

## Pull data (optional)

To re-pull from BigQuery (~$16):

```bash
bun scripts/pull-readmes.ts ./data
```
