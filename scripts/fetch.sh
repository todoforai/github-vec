#!/bin/bash
# Usage: ./scripts/fetch.sh [options]
# Auto-loads proxies.txt and proxies_2.txt if present
#
# For fastest fetching, run parallel instances in separate terminals:
#   byobu new-window -n "60-80"   "./scripts/fetch.sh --offset=60000000 --limit=20000000"
#   byobu new-window -n "80-100"  "./scripts/fetch.sh --offset=80000000 --limit=20000000"
#   byobu new-window -n "100-120" "./scripts/fetch.sh --offset=100000000 --limit=20000000"
#   # ... etc. Each instance saves progress and resumes on restart.

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

PROXY_ARGS=""
for f in "$REPO_ROOT"/proxies*.txt; do
  [ -f "$f" ] && PROXY_ARGS="$PROXY_ARGS --proxies=$f"
done

exec bun scripts/fetch-readmes.ts --full $PROXY_ARGS "$@"
