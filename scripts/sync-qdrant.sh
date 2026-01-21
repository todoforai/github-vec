#!/bin/bash
set -e

HOST="${QDRANT_HOST:?Set QDRANT_HOST env var}"

echo "Stopping remote Qdrant..."
ssh "$HOST" "systemctl stop qdrant"

echo "Syncing Qdrant storage (this may take a while)..."
rsync -avz --progress storage/ "$HOST":/var/lib/qdrant/storage/

echo "Starting remote Qdrant..."
ssh "$HOST" "systemctl start qdrant"

echo "Done! Verifying collections..."
ssh "$HOST" "curl -s http://localhost:6333/collections"
