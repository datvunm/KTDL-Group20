#!/usr/bin/env bash
# source/load_dump.sh
# Host script (any cwd): recreates the demo DB, restores the committed raw dump
# (3 denormalized tables, full history) into schema archive, creates the empty
# raw schema and initializes simulator state. Idempotent: re-running fully resets.
#
# The raw dump is committed as gzip parts in source/data/raw/ (built once by
# source/export_raw_dump.sh from the official demo-medium-en-20170815 dump).
#
# Environment variables:
#   PSQL_EXEC   - override psql command (default: docker compose ... exec -T db psql -U postgres)
#   DUMP_DIR    - override directory holding the raw dump parts (default: source/data/raw)
#   SCHEMA_FILE - override schema.sql path (host path)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DUMP_DIR="${DUMP_DIR:-$SCRIPT_DIR/data/raw}"
DUMP_PARTS_GLOB="*.sql.gz.part-*"

# 1. Resolve and verify the raw dump parts (read on the host, streamed into psql)
shopt -s nullglob
DUMP_PARTS=("$DUMP_DIR"/$DUMP_PARTS_GLOB)
shopt -u nullglob
if [ ${#DUMP_PARTS[@]} -eq 0 ]; then
    echo "Error: no raw dump parts ($DUMP_PARTS_GLOB) found in $DUMP_DIR" >&2
    exit 1
fi
if [ -f "$DUMP_DIR/SHA256SUMS" ]; then
    echo "Verifying raw dump checksums..."
    if command -v sha256sum >/dev/null 2>&1; then
        (cd "$DUMP_DIR" && sha256sum -c --quiet SHA256SUMS)
    else
        # macOS has shasum instead of sha256sum
        (cd "$DUMP_DIR" && shasum -a 256 -c --quiet SHA256SUMS)
    fi
fi

# 2. Configure execution parameters. SQL files are streamed to psql on stdin from
# the host, so no container path is needed (also avoids Git Bash rewriting
# /source/... paths on Windows).
PSQL_EXEC="${PSQL_EXEC:-docker compose -f $REPO_ROOT/postgres/docker-compose.dev.yaml exec -T db psql -U postgres}"
SCHEMA_SQL="${SCHEMA_FILE:-$SCRIPT_DIR/schema.sql}"

echo "============================================================"
echo "Initializing demo database..."
echo "PSQL_EXEC: $PSQL_EXEC"
echo "Dump parts: ${DUMP_PARTS[*]}"
echo "Schema file: $SCHEMA_SQL"
echo "============================================================"

# Step 1: Recreate demo database
echo "[Step 1/3] Recreating demo database..."
$PSQL_EXEC -d postgres -c "DROP DATABASE IF EXISTS demo WITH (FORCE);"
$PSQL_EXEC -d postgres -c "CREATE DATABASE demo;"

# Step 2: Restore raw dump into schema archive (parts are one gzip stream, in order)
echo "[Step 2/3] Restoring raw dump into schema archive..."
cat "${DUMP_PARTS[@]}" | gunzip | $PSQL_EXEC -d demo -v ON_ERROR_STOP=1 -q

# Step 3: Initialize empty raw schema and simulator state
echo "[Step 3/3] Initializing raw schema for the simulator..."
$PSQL_EXEC -d demo -v ON_ERROR_STOP=1 < "$SCHEMA_SQL"
$PSQL_EXEC -d demo -c "ANALYZE;"

echo ""
echo "Database demo initialized successfully."
echo "archive schema contains the full-history raw tables."
echo "raw schema contains empty source tables ready for simulator."
$PSQL_EXEC -d demo -c "SELECT raw.now() AS initial_simulation_cutoff;"
