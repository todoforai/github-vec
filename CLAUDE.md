# github-vec

Semantic search for GitHub repos using vector embeddings.

## Stack

- **Runtime**: Bun (not Node)
- **Frontend**: React + Vite + Tailwind
- **Backend**: Bun.serve() API
- **Database**: Qdrant vector DB
- **Embeddings**: OpenRouter (Qwen3-Embedding-8B)

## Infrastructure

- **todoforai** (65.108.11.117) - API server, runs backend + frontend
- **todoforai_db** (135.181.61.40) - DB server, runs Qdrant on `/var/lib/postgresql/qdrant/`
- **Repo on server**: `/home/root/github-vec`

## Local dev with remote Qdrant

SSH tunnel to access remote Qdrant:

```bash
ssh -L 6334:localhost:6333 todoforai_db -N &
QDRANT_URL=http://localhost:6334 bun scripts/ingest.ts
```

## Scripts

- `scripts/ingest.ts` - Ingest readmes into Qdrant
- `scripts/sync-qdrant.sh` - Sync local Qdrant storage to remote server

## Deployment

Push to master triggers GitHub Actions deploy to todoforai.
