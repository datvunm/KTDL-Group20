#!/usr/bin/env bash
# dbt/generate_docs.sh
# Compiles dbt models and generates static documentation assets (manifest.json, catalog.json, index.html)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================================"
echo "Generating dbt documentation assets..."
echo "Project directory: $SCRIPT_DIR"
echo "============================================================"

# Check for dbt CLI
if command -v dbt >/dev/null 2>&1; then
    DBT_CMD="dbt"
elif [ -f "/tmp/ktdl-dbt-venv/bin/dbt" ]; then
    DBT_CMD="/tmp/ktdl-dbt-venv/bin/dbt"
elif [ -f "$SCRIPT_DIR/venv/bin/dbt" ]; then
    DBT_CMD="$SCRIPT_DIR/venv/bin/dbt"
else
    echo "dbt not found in PATH or standard venvs. Creating temporary runner..."
    uv venv /tmp/ktdl-dbt-venv
    uv pip install --python /tmp/ktdl-dbt-venv/bin/python dbt-core dbt-spark dbt-duckdb
    DBT_CMD="/tmp/ktdl-dbt-venv/bin/dbt"
fi

echo "Using dbt binary: $DBT_CMD"
$DBT_CMD --version

echo ""
echo "Step 1/2: Compiling models..."
$DBT_CMD compile --project-dir "$SCRIPT_DIR" --profiles-dir "$SCRIPT_DIR" --target dev

echo ""
echo "Step 2/2: Building catalog and documentation..."
$DBT_CMD docs generate --project-dir "$SCRIPT_DIR" --profiles-dir "$SCRIPT_DIR" --target dev

echo ""
echo "Documentation assets generated in: $SCRIPT_DIR/target"
ls -lh "$SCRIPT_DIR/target/index.html" "$SCRIPT_DIR/target/manifest.json" "$SCRIPT_DIR/target/catalog.json"
echo "dbt documentation ready to be served."
