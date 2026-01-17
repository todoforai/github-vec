#!/bin/bash
# Usage: ./scripts/fetch.sh [options]
# Auto-loads proxies.txt and proxies_2.txt if present

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

PROXY_ARGS=""
for f in "$REPO_ROOT"/proxies*.txt; do
  [ -f "$f" ] && PROXY_ARGS="$PROXY_ARGS --proxies=$f"
done

exec bun scripts/fetch-readmes.ts --full $PROXY_ARGS "$@"
