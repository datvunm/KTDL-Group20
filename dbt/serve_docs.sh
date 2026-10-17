#!/usr/bin/env bash
# dbt/serve_docs.sh
# Serves dbt documentation site on host port 8085 (standalone without Docker)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${DBT_DOCS_PORT:-8085}"

if [ ! -f "$SCRIPT_DIR/target/index.html" ]; then
    echo "Documentation not found in $SCRIPT_DIR/target. Generating first..."
    "$SCRIPT_DIR/generate_docs.sh"
fi

echo "============================================================"
echo "Starting local dbt docs server..."
echo "Serving: $SCRIPT_DIR/target"
echo "URL: http://localhost:$PORT"
echo "Press Ctrl+C to stop."
echo "============================================================"

exec python3 -m http.server "$PORT" --directory "$SCRIPT_DIR/target"
