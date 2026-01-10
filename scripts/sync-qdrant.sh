#!/bin/bash
set -e

echo "Stopping remote Qdrant..."
ssh todoforai "systemctl stop qdrant"

echo "Syncing Qdrant storage (this may take a while)..."
rsync -avz --progress storage/ todoforai:/var/lib/qdrant/storage/

echo "Starting remote Qdrant..."
ssh todoforai "systemctl start qdrant"

echo "Done! Verifying collections..."
ssh todoforai "curl -s http://localhost:6333/collections"
