#!/bin/bash
# Usage: ./scripts/fetch.sh [options]
# Auto-loads proxies.txt if present

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROXIES="$REPO_ROOT/proxies.txt"

if [ -f "$PROXIES" ]; then
  exec bun scripts/fetch-readmes.ts --full --proxies="$PROXIES" "$@"
else
  exec bun scripts/fetch-readmes.ts --full "$@"
fi
