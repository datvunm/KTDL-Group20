#!/usr/bin/env bash

# Replays a bounded slice of the archive through both producers, in real
# chronological order, one after the other (not concurrently - spark-stream
# only has 1 core, so running both at once just piles up a bigger backlog
# for it to chew through at the same time).
#
# Requires ./start-streaming.sh already running.

# Knobs (env vars): LIMIT_BOOKINGS, LIMIT_FLIGHTS, SPEED_FACTOR,
# MIN_GAP_SECONDS, MAX_GAP_SECONDS - see kafka/producer/*.py.

set -e

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export LIMIT_BOOKINGS="${LIMIT_BOOKINGS:-5000}"
export LIMIT_FLIGHTS="${LIMIT_FLIGHTS:-1000}"
export SPEED_FACTOR="${SPEED_FACTOR:-4000}"
export MIN_GAP_SECONDS="${MIN_GAP_SECONDS:-0.05}"
export MAX_GAP_SECONDS="${MAX_GAP_SECONDS:-2.0}"

echo "--- Replaying LIMIT_BOOKINGS=$LIMIT_BOOKINGS, LIMIT_FLIGHTS=$LIMIT_FLIGHTS at SPEED_FACTOR=$SPEED_FACTOR"
echo "--- (unset LIMIT_BOOKINGS/LIMIT_FLIGHTS for the full archive - only after confirming a small run is healthy)"
echo "--- Ctrl+C to stop early."
echo

echo "--- Bookings:"
docker compose -f "$root/kafka/docker-compose.dev.yaml" run --rm booking-producer

echo "--- Flights:"
docker compose -f "$root/kafka/docker-compose.dev.yaml" run --rm flight-producer

echo "--- Done."
