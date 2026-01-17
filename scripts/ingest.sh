#!/bin/bash
# Usage: ./scripts/ingest.sh [parallel_workers] [provider]
# Examples:
#   ./scripts/ingest.sh 20
#   ./scripts/ingest.sh 20 nebius-batch
#   ./scripts/ingest.sh 20 deepinfra

PARALLEL=${1:-20}
PROVIDER=${2:-nebius-batch}

exec bun scripts/ingest.ts --provider="$PROVIDER" --parallel="$PARALLEL"
