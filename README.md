# github-vec

GitHub READMEs, vectorized.

> *"Ever searched GitHub for a project you knew existed but couldn't find?"*
>
> *"You remember the concept, maybe a few keywords, but GitHub search returns nothing."*

I got frustrated enough to embed 23M unique GitHub READMEs into a vector database. Now you can search by *meaning*, not just keywords.

Designed to work with claude-code subagents, keeping contexts lean.

## CLI

```bash
# Install globally
bun install -g github-vec

# Search by meaning
github-vec "vector database for embeddings"
github-vec "lightweight web framework" --limit 20
```

Options:
- `-l, --limit <n>` - Number of results (default: 10, max: 50)
- `-h, --help` - Show help

Uses hosted API at `https://github-vec.com`

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

## Service Status

**Paused** (as of 2026-06). Traffic had dropped to a handful of searches/month, so both services were stopped:

- Backend `github-vec.service` on `api.todofor.ai` — stopped & disabled
- Qdrant `qdrant.service` on `db.todofor.ai` — stopped & disabled (storage `/var/lib/qdrant/storage/` ~350 GB kept intact)

The site shows a "service paused" banner pointing to `marcellhavlik@todofor.ai`.

### How to restart

```bash
# 1. Bring Qdrant back (db.todofor.ai) — data is still on disk
ssh todoforai_db 'systemctl enable --now qdrant'

# 2. Bring the backend back (api.todofor.ai)
ssh todoforai 'systemctl enable --now github-vec'

# 3. Verify
curl -s 'https://github-vec.com/search?q=fast+rust+terminal' | head
```

Then remove the "service paused" banner in `frontend/src/App.tsx` (`HeroHeader`), rebuild and redeploy:

```bash
cd frontend && bun run build
rsync -avz --delete dist/ todoforai:/var/www/github-vec/dist/
```

Usage analytics live in SQLite on the backend host: `/var/www/github-vec/backend/data/analytics.db` (`searches` table — see `backend/analytics.ts`).
