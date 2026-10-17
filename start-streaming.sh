#!/usr/bin/env bash

# Starts the streaming speed layer on demand: Kafka (single broker) and the
# combined Spark Structured Streaming app (booking + flight telemetry).
# These only matter while you're demonstrating live ingestion - stop them
# when you're done to free up resources:
#   docker stop spark-spark-stream-1 kafka-kafka-0-1
#
# Requires the core stack (./start-all.sh) already running - spark-stream
# submits to the existing spark master.

set -e

# Git Bash on Windows rewrites /opt/... args into Windows paths before they
# reach docker exec; exclude just that prefix, so our own $root/... paths
# below still convert normally.
export MSYS2_ARG_CONV_EXCL="/opt/"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "--- Starting Kafka broker"
docker compose -f "$root/kafka/docker-compose.dev.yaml" up -d --build kafka-0

echo "--- Waiting for Kafka to accept connections..."
for i in $(seq 1 15); do
  if docker exec kafka-kafka-0-1 /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka-0:9092 >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

# Spark's Kafka source refuses to subscribe to a topic that doesn't exist
# yet, so these must exist before spark-stream starts.
echo "--- Creating topics (if missing)"
docker exec kafka-kafka-0-1 /opt/kafka/bin/kafka-topics.sh --create --topic booking.created \
  --bootstrap-server kafka-0:9092 --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka-kafka-0-1 /opt/kafka/bin/kafka-topics.sh --create --topic flight.departed \
  --bootstrap-server kafka-0:9092 --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka-kafka-0-1 /opt/kafka/bin/kafka-topics.sh --create --topic flight.arrived \
  --bootstrap-server kafka-0:9092 --partitions 3 --replication-factor 1 --if-not-exists

echo "--- Starting the combined Spark streaming app (booking + flight telemetry)"
docker compose -f "$root/spark/docker-compose.dev.yaml" up -d spark-stream

echo
echo "--- Streaming layer is up. To send test data:"
echo "  docker compose -f kafka/docker-compose.dev.yaml run --rm -e LIMIT_BOOKINGS=50 -e SPEED_FACTOR=200000 -e MAX_GAP_SECONDS=0.02 booking-producer"
echo "  docker compose -f kafka/docker-compose.dev.yaml run --rm -e LIMIT_FLIGHTS=50 -e SPEED_FACTOR=200000 -e MAX_GAP_SECONDS=0.02 flight-producer"
echo
echo "--- Watch it process:"
echo "  docker logs -f spark-spark-stream-1"
echo
echo "--- When done, stop it:"
echo "  docker stop spark-spark-stream-1 kafka-kafka-0-1"
