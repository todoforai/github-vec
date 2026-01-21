# github-vec

Semantic search for GitHub repos using vector embeddings.

## Stack

- **Runtime**: Bun (not Node)
- **Frontend**: React + Vite + Tailwind
- **Backend**: Bun.serve() API
- **Database**: Qdrant vector DB
- **Embeddings**: OpenRouter (Qwen3-Embedding-8B)

## Scripts

- `scripts/ingest.ts` - Ingest readmes into Qdrant
- `scripts/sync-qdrant.sh` - Sync local Qdrant storage to remote server

## Deployment

Push to master triggers GitHub Actions deploy.
