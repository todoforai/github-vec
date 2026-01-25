# github-vec

GitHub READMEs, vectorized.

> *"Ever searched GitHub for a project you knew existed but couldn't find?"*
>
> *"You remember the concept, maybe a few keywords, but GitHub search returns nothing."*

I got frustrated enough to embed 23M unique GitHub READMEs into a vector database. Now you can search by *meaning*, not just keywords.

Designed to work with claude-code subagents, keeping contexts lean.

## MCP Server

Add to your Claude Code config (`~/.claude.json`):

```json
{
  "mcpServers": {
    "github-vec": {
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "github:todoforai/github-vec-mcp"]
    }
  }
}
```

Or run directly:
```bash
npx -y github:todoforai/github-vec-mcp
```

## Why use this

> *"Someone already made something like your project. You just can't find it."*
>
> *"Stop reinventing. Start finding."*

## Setup

```bash
./setup.sh              # Install deps + Qdrant
qdrant                  # Start Qdrant server (in separate terminal)
bun scripts/ingest.ts   # Ingest READMEs into Qdrant
```

Requires:
- `DEEPINFRA_API_KEY` - for embeddings
- `DATA_DIR` - path to data directory (default: `/home/root/data`)

## Qdrant Servers

| Server | URL | Description |
|--------|-----|-------------|
| Local | `http://localhost:6333` | Default development instance |
| Production | `http://db.todofor.ai:6333` | Remote production instance |

To ingest to production:

```bash
QDRANT_URL="http://db.todofor.ai:6333" bun scripts/ingest.ts
```

To sync local storage to production (stops remote Qdrant, rsyncs, restarts):

```bash
./scripts/sync-qdrant.sh
```

## Data

| Property | Value |
|----------|-------|
| Records | 23M unique READMEs (100M+ with forks) |
| Size | ~350 GB |
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
