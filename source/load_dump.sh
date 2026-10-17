#!/usr/bin/env bash
# source/load_dump.sh
# Host script (any cwd): downloads demo dump if missing, pre-creates demo DB,
# restores dump (renaming schema bookings -> archive), creates empty bookings schema,
# and initializes simulator state. Idempotent: re-running fully resets.
#
# Environment variables:
#   PSQL_EXEC   - override psql command (default: docker compose ... exec -T db psql -U postgres)
#   DUMP_FILE   - override dump path (default: source/data/demo-medium-en-20170815.sql)
#   SCHEMA_FILE - override schema.sql path

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="$SCRIPT_DIR/data"

DUMP_URL="https://edu.postgrespro.com/demo-medium-en.zip"
DEFAULT_DUMP_FILE="$DATA_DIR/demo-medium-en-20170815.sql"

# 1. Resolve dump file and download if missing
if [ -n "${DUMP_FILE:-}" ]; then
    LOCAL_DUMP="$DUMP_FILE"
    echo "Using user-specified dump file: $LOCAL_DUMP"
else
    mkdir -p "$DATA_DIR"
    LOCAL_DUMP="$DEFAULT_DUMP_FILE"
    if [ ! -f "$LOCAL_DUMP" ]; then
        echo "Dump file not found at $LOCAL_DUMP. Downloading from $DUMP_URL..."
        ZIP_FILE="$DATA_DIR/demo-medium-en.zip"
        if command -v curl >/dev/null 2>&1; then
            curl -fsSL -o "$ZIP_FILE" "$DUMP_URL"
        elif command -v wget >/dev/null 2>&1; then
            wget -q -O "$ZIP_FILE" "$DUMP_URL"
        else
            echo "Error: Neither curl nor wget is available to download dump." >&2
            exit 1
        fi
        echo "Extracting dump file..."
        unzip -o -d "$DATA_DIR" "$ZIP_FILE"
        rm -f "$ZIP_FILE"
        echo "Dump extracted to $LOCAL_DUMP"
    else
        echo "Dump file already exists at $LOCAL_DUMP. Skipping download."
    fi
fi

# 2. Configure execution parameters
if [ -n "${PSQL_EXEC:-}" ]; then
    PSQL_DUMP_PATH="${PSQL_DUMP_PATH:-$LOCAL_DUMP}"
    SCHEMA_SQL="${SCHEMA_FILE:-$SCRIPT_DIR/schema.sql}"
else
    PSQL_EXEC="docker compose -f $REPO_ROOT/postgres/docker-compose.dev.yaml exec -T db psql -U postgres"
    # When running inside docker container, /source is mounted
    PSQL_DUMP_PATH="${PSQL_DUMP_PATH:-/source/data/$(basename "$LOCAL_DUMP")}"
    SCHEMA_SQL="${SCHEMA_FILE:-/source/schema.sql}"
fi

echo "============================================================"
echo "Initializing demo database..."
echo "PSQL_EXEC: $PSQL_EXEC"
echo "Dump file: $PSQL_DUMP_PATH"
echo "Schema file: $SCHEMA_SQL"
echo "============================================================"

# Step 1: Pre-create demo database connected to postgres db
# The dump contains DROP DATABASE demo; CREATE DATABASE demo; \connect demo
# If demo database does not exist initially, DROP DATABASE demo fails unless pre-created.
echo "[Step 1/3] Pre-creating demo database..."
$PSQL_EXEC -d postgres -c "DROP DATABASE IF EXISTS demo WITH (FORCE);"
$PSQL_EXEC -d postgres -c "CREATE DATABASE demo;"

# Step 2: Restore dump (connected to postgres db initially; dump script connects to demo)
echo "[Step 2/3] Restoring demo database dump..."
$PSQL_EXEC -d postgres -v ON_ERROR_STOP=1 -f "$PSQL_DUMP_PATH"

# Step 3: Rename restored bookings schema to archive and set up empty bookings schema
echo "[Step 3/3] Renaming bookings schema to archive and initializing simulator schema..."
$PSQL_EXEC -d demo -c "ALTER SCHEMA bookings RENAME TO archive;"
$PSQL_EXEC -d demo -c "CREATE INDEX IF NOT EXISTS tickets_book_ref_idx ON archive.tickets (book_ref);"
$PSQL_EXEC -d demo -c "CREATE INDEX IF NOT EXISTS bookings_book_date_idx ON archive.bookings (book_date);"
$PSQL_EXEC -d demo -c "CREATE INDEX IF NOT EXISTS flights_scheduled_departure_idx ON archive.flights (scheduled_departure);"
$PSQL_EXEC -d demo -v ON_ERROR_STOP=1 -f "$SCHEMA_SQL"
$PSQL_EXEC -d demo -c "ANALYZE;"

echo ""
echo "Database demo initialized successfully."
echo "archive schema contains historical baseline data."
echo "bookings schema contains empty operational tables ready for simulator."
$PSQL_EXEC -d demo -c "SELECT bookings.now() AS initial_simulation_cutoff;"
