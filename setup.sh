#!/bin/bash
set -e

QDRANT_VERSION="1.16.3"
INSTALL_DIR="${HOME}/.local/bin"

echo "=== github-vec setup ==="

bun install

if command -v qdrant &> /dev/null; then
  echo "Qdrant already installed: $(which qdrant)"
else
  echo "Installing Qdrant v${QDRANT_VERSION}..."
  mkdir -p "$INSTALL_DIR"
  curl -sL "https://github.com/qdrant/qdrant/releases/download/v${QDRANT_VERSION}/qdrant-x86_64-unknown-linux-musl.tar.gz" | tar xz -C "$INSTALL_DIR"
  echo "Installed to ${INSTALL_DIR}/qdrant"
fi

if [ -z "$OPENROUTER_API_KEY" ]; then
  echo ""
  echo "WARNING: OPENROUTER_API_KEY not set"
  echo "Get one at: https://openrouter.ai/keys"
  echo "Then: export OPENROUTER_API_KEY=your_key"
fi

echo ""
echo "=== Setup complete ==="
echo ""
echo "Usage:"
echo "  qdrant                              # Start Qdrant server"
echo "  bun scripts/ingest.ts               # Ingest READMEs"
echo "  bun scripts/pull-readmes.ts ./data  # Pull from BigQuery"
