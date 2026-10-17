#!/usr/bin/env bash
# source/simulate.sh
# Runs simulator for a given cutoff timestamp in the demo database,
# then displays row counts for all 8 bookings tables and flight status distribution.
#
# Usage:
#   ./simulate.sh 2017-06-15
#   ./simulate.sh '2017-08-15 18:00:00+03'
#
# Environment variables:
#   PSQL_EXEC    - command to invoke psql (default: docker compose ... exec -T db psql -U postgres)
#   SIMULATE_SQL - path to simulate.sql (default: /source/simulate.sql or local file when PSQL_EXEC is set)

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

if [ -n "${PSQL_EXEC:-}" ]; then
    SIMULATE_SQL="${SIMULATE_SQL:-$SCRIPT_DIR/simulate.sql}"
else
    PSQL_EXEC="docker compose -f $REPO_ROOT/postgres/docker-compose.dev.yaml exec -T db psql -U postgres"
    SIMULATE_SQL="${SIMULATE_SQL:-/source/simulate.sql}"
fi

echo "============================================================"
echo "Starting simulation for cutoff: $CUTOFF"
echo "SQL script: $SIMULATE_SQL"
echo "============================================================"

# Execute simulation script
$PSQL_EXEC -d demo -v ON_ERROR_STOP=1 -v cutoff="'$CUTOFF'" -f "$SIMULATE_SQL"

echo ""
echo "=== Bookings Schema State (Cutoff: $CUTOFF) ==="
$PSQL_EXEC -d demo -c "
SELECT
    current_setting('search_path') AS search_path,
    bookings.now() AS bookings_now;
"

echo ""
echo "=== Table Row Counts ==="
$PSQL_EXEC -d demo -c "
SELECT 'aircrafts_data' AS table_name, count(*) AS row_count FROM bookings.aircrafts_data
UNION ALL
SELECT 'airports_data', count(*) FROM bookings.airports_data
UNION ALL
SELECT 'seats', count(*) FROM bookings.seats
UNION ALL
SELECT 'bookings', count(*) FROM bookings.bookings
UNION ALL
SELECT 'tickets', count(*) FROM bookings.tickets
UNION ALL
SELECT 'flights', count(*) FROM bookings.flights
UNION ALL
SELECT 'ticket_flights', count(*) FROM bookings.ticket_flights
UNION ALL
SELECT 'boarding_passes', count(*) FROM bookings.boarding_passes;
"

echo ""
echo "=== Flight Status Distribution ==="
$PSQL_EXEC -d demo -c "
SELECT
    status,
    count(*) AS count
FROM bookings.flights
GROUP BY status
ORDER BY status;
"
