#!/usr/bin/env bash
# source/export_raw_dump.sh
# One-time tool: converts the official demo dump (8 normalized tables) into the
# 3 denormalized raw tables (source/build_raw.sql) and writes them as the
# committed raw dump source/data/raw/demo-medium-en-20170815-raw.sql.gz.part-NN
# (gzip, split below GitHub's 100 MB file limit) plus SHA256SUMS.
# load_dump.sh restores these parts into schema archive.
#
# Only needed to regenerate the committed raw dump; not part of the normal flow.
#
# Usage:
#   ./source/export_raw_dump.sh                 # build from the original dump
#   ./source/export_raw_dump.sh --from-db demo  # export raw.* already built in database demo
#                                               # (must be at the final cutoff 2017-08-15 18:00:00+03)
#
# Environment variables:
#   PSQL_EXEC    - command to invoke psql    (default: docker compose ... exec -T db psql -U postgres)
#   PG_DUMP_EXEC - command to invoke pg_dump (default: docker compose ... exec -T db pg_dump -U postgres)
#   BUILD_RAW_SQL - host path to build_raw.sql (default: source/build_raw.sql)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="$SCRIPT_DIR/data"
OUT_DIR="$DATA_DIR/raw"
OUT_PREFIX="demo-medium-en-20170815-raw.sql.gz.part-"
PART_SIZE="90m"

DUMP_URL="https://edu.postgrespro.com/demo-medium-en.zip"
ORIG_NAME="demo-medium-en-20170815.sql"
FINAL_CUTOFF="2017-08-15 18:00:00+03"

BUILD_DB="demo_build"
EXPORT_DB="raw_export"
RAW_TABLES=(flight_seat_reservations airport_sites aircraft_seat_layouts)

FROM_DB=""
if [ "${1:-}" = "--from-db" ]; then
    FROM_DB="${2:?--from-db needs a database name}"
fi

# SQL files are streamed to psql on stdin from the host (no container paths)
COMPOSE="docker compose -f $REPO_ROOT/postgres/docker-compose.dev.yaml"
PSQL_EXEC="${PSQL_EXEC:-$COMPOSE exec -T db psql -U postgres}"
PG_DUMP_EXEC="${PG_DUMP_EXEC:-$COMPOSE exec -T db pg_dump -U postgres}"
BUILD_RAW_SQL="${BUILD_RAW_SQL:-$SCRIPT_DIR/build_raw.sql}"

# 1. Source database with raw.* built from the original dump
if [ -z "$FROM_DB" ]; then
    if [ ! -f "$DATA_DIR/$ORIG_NAME" ]; then
        echo "[1/4] Downloading original dump from $DUMP_URL..."
        mkdir -p "$DATA_DIR"
        curl -fsSL -o "$DATA_DIR/demo-medium-en.zip" "$DUMP_URL"
        unzip -o -d "$DATA_DIR" "$DATA_DIR/demo-medium-en.zip"
        rm -f "$DATA_DIR/demo-medium-en.zip"
    fi

    echo "[1/4] Restoring original dump into $BUILD_DB and building raw.*..."
    $PSQL_EXEC -d postgres -c "DROP DATABASE IF EXISTS $BUILD_DB WITH (FORCE);"
    $PSQL_EXEC -d postgres -c "CREATE DATABASE $BUILD_DB;"
    # The dump drops, creates, alters and connects to database demo: strip those
    # lines so it restores into BUILD_DB and never touches demo.
    # ([\]connect, not \\connect: GNU sed reads \c as a control-char escape)
    sed -E -e '/^(DROP|CREATE|ALTER) DATABASE demo/d' -e '/^[\]connect demo/d' "$DATA_DIR/$ORIG_NAME" \
        | $PSQL_EXEC -d "$BUILD_DB" -v ON_ERROR_STOP=1 -q > /dev/null
    # The original dump leaves the full final state in schema bookings
    # (identical to the simulator at its final cutoff).
    $PSQL_EXEC -d "$BUILD_DB" -v ON_ERROR_STOP=1 \
        -c "CREATE SCHEMA IF NOT EXISTS raw" \
        -c "SET search_path = raw, bookings" \
        -f - < "$BUILD_RAW_SQL"
    $PSQL_EXEC -d "$BUILD_DB" -v ON_ERROR_STOP=1 -c "
DO \$\$
BEGIN
    IF (SELECT count(*) FROM raw.flight_seat_reservations WHERE ticket_no IS NOT NULL)
       <> (SELECT count(*) FROM bookings.ticket_flights) THEN
        RAISE EXCEPTION 'raw booked rows do not match bookings.ticket_flights';
    END IF;
END
\$\$;"
    FROM_DB="$BUILD_DB"
else
    echo "[1/4] Using raw.* already built in database $FROM_DB"
    # The archive must hold the full history: refuse raw.* at an earlier cutoff.
    # (The clock is raw.now(), or bookings.now() in the original 8-table setup.)
    $PSQL_EXEC -d "$FROM_DB" -v ON_ERROR_STOP=1 -c "
DO \$\$
DECLARE c timestamptz;
BEGIN
    IF to_regproc('raw.now') IS NOT NULL THEN
        EXECUTE 'SELECT raw.now()' INTO c;
    ELSE
        EXECUTE 'SELECT bookings.now()' INTO c;
    END IF;
    IF c IS DISTINCT FROM '$FINAL_CUTOFF'::timestamptz THEN
        RAISE EXCEPTION 'raw.* in % is at cutoff %, not the final cutoff $FINAL_CUTOFF', current_database(), c;
    END IF;
END
\$\$;"
fi

# 2. Copy the 3 raw tables into a scratch database and rename raw -> archive
echo "[2/4] Copying raw.* from $FROM_DB into $EXPORT_DB as archive.*..."
$PSQL_EXEC -d postgres -c "DROP DATABASE IF EXISTS $EXPORT_DB WITH (FORCE);"
$PSQL_EXEC -d postgres -c "CREATE DATABASE $EXPORT_DB;"
$PSQL_EXEC -d "$EXPORT_DB" -c "CREATE SCHEMA raw;"
table_args=()
for t in "${RAW_TABLES[@]}"; do table_args+=(-t "raw.$t"); done
$PG_DUMP_EXEC -d "$FROM_DB" --no-owner --no-privileges "${table_args[@]}" \
    | $PSQL_EXEC -d "$EXPORT_DB" -v ON_ERROR_STOP=1 -q
$PSQL_EXEC -d "$EXPORT_DB" -v ON_ERROR_STOP=1 \
    -c "DROP INDEX IF EXISTS raw.flight_seat_reservations_flight_id_idx" \
    -c "ALTER SCHEMA raw RENAME TO archive"

# 3. Dump, compress and split
echo "[3/4] Writing $OUT_DIR/${OUT_PREFIX}NN..."
mkdir -p "$OUT_DIR"
rm -f "$OUT_DIR/${OUT_PREFIX}"* "$OUT_DIR/SHA256SUMS"
$PG_DUMP_EXEC -d "$EXPORT_DB" --no-owner --no-privileges -n archive \
    | gzip -n -9 \
    | split -b "$PART_SIZE" -d -a 2 - "$OUT_DIR/$OUT_PREFIX"
(cd "$OUT_DIR" && sha256sum "${OUT_PREFIX}"* > SHA256SUMS)

# 4. Clean up scratch databases
echo "[4/4] Dropping scratch databases..."
$PSQL_EXEC -d postgres -c "DROP DATABASE IF EXISTS $EXPORT_DB WITH (FORCE);"
$PSQL_EXEC -d postgres -c "DROP DATABASE IF EXISTS $BUILD_DB WITH (FORCE);"

echo ""
echo "Raw dump written:"
ls -l "$OUT_DIR"
