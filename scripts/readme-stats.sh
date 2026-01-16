#!/bin/bash
# Report README format stats from fetched data

READMES_DIR="${1:-/home/root/data/readmes}"

echo "=== README Format Stats ==="
echo ""
echo "API fallbacks by format:"
ls "$READMES_DIR" | grep "_default_" | sed 's/.*_default_//' | sort | uniq -c | sort -rn
echo ""
echo "All formats (by branch):"
ls "$READMES_DIR" | grep -oE '_(main|master|default)_.*$' | sed 's/^_//' | sort | uniq -c | sort -rn
echo ""
echo "---"
echo "Total API fallbacks: $(ls "$READMES_DIR" | grep -c '_default_')"
echo "Total READMEs: $(ls "$READMES_DIR" | wc -l)"
