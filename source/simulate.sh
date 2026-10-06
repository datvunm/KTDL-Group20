#!/usr/bin/env bash
# source/simulate.sh
# Runs simulator for a given cutoff timestamp in the demo database,
# then displays row counts for the 3 raw tables and flight status distribution.
#
# Usage:
#   ./simulate.sh 2017-06-15
#   ./simulate.sh '2017-08-15 18:00:00+03'
#
# Environment variables:
#   PSQL_EXEC    - command to invoke psql (default: docker compose ... exec -T db psql -U postgres)
#   SIMULATE_SQL - host path to simulate.sql (default: source/simulate.sql next to this script).
#                  The file is streamed to psql on stdin, so no container path is needed
#                  (also avoids Git Bash rewriting /source/... paths on Windows).

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <cutoff>" >&2
    echo "Examples:" >&2
    echo "  $0 2017-06-15" >&2
    echo "  $0 '2017-08-15 18:00:00+03'" >&2
    exit 1
fi

CUTOFF="$*"
# Strip surrounding single or double quotes if provided
while [[ "$CUTOFF" =~ ^[\'\"].*[\'\"]$ ]]; do
    CUTOFF="${CUTOFF:1:-1}"
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PSQL_EXEC="${PSQL_EXEC:-docker compose -f $REPO_ROOT/postgres/docker-compose.dev.yaml exec -T db psql -U postgres}"
SIMULATE_SQL="${SIMULATE_SQL:-$SCRIPT_DIR/simulate.sql}"

echo "============================================================"
echo "Starting simulation for cutoff: $CUTOFF"
echo "SQL script: $SIMULATE_SQL"
echo "============================================================"

# Execute simulation script
$PSQL_EXEC -d demo -v ON_ERROR_STOP=1 -v cutoff="'$CUTOFF'" < "$SIMULATE_SQL"

echo ""
echo "=== Raw Schema State (Cutoff: $CUTOFF) ==="
$PSQL_EXEC -d demo -c "
SELECT
    current_setting('search_path') AS search_path,
    raw.now() AS raw_now;
"

echo ""
echo "=== Table Row Counts ==="
$PSQL_EXEC -d demo -c "
SELECT 'flight_seat_reservations' AS table_name, count(*) AS row_count FROM raw.flight_seat_reservations
UNION ALL
SELECT 'airport_sites', count(*) FROM raw.airport_sites
UNION ALL
SELECT 'aircraft_seat_layouts', count(*) FROM raw.aircraft_seat_layouts;
"

echo ""
echo "=== Reservation Rows ==="
$PSQL_EXEC -d demo -c "
SELECT
    count(DISTINCT flight_id) AS flights,
    count(*) FILTER (WHERE ticket_no IS NOT NULL) AS booked_rows,
    count(*) FILTER (WHERE ticket_no IS NOT NULL AND seat_no IS NOT NULL) AS seated_rows,
    count(*) FILTER (WHERE ticket_no IS NOT NULL AND seat_no IS NULL) AS unseated_rows,
    count(*) FILTER (WHERE ticket_no IS NULL) AS empty_seat_rows,
    count(DISTINCT book_ref) AS bookings
FROM raw.flight_seat_reservations;
"

echo ""
echo "=== Flight Status Distribution ==="
$PSQL_EXEC -d demo -c "
SELECT
    status,
    count(DISTINCT flight_id) AS count
FROM raw.flight_seat_reservations
GROUP BY status
ORDER BY status;
"
