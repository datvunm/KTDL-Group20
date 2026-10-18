#!/usr/bin/env bash

set -e

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
network="ktdl-network"

# Core pipeline only - admin UIs (adminer, mongo-express, dbt-docs) and the
# streaming layer (kafka, spark-stream) are heavy and only needed when
# you're actually using them. Bring them up with:
#   ./start-admin-uis.sh
#   ./start-streaming.sh
# "file|services" - empty services means every service in that file.
stacks=(
  "postgres/docker-compose.dev.yaml|db"
  "mongo/docker-compose.dev.yaml|mongo"
  "hadoop/docker-compose.dev.yaml|"
  "spark/docker-compose.dev.yaml|spark spark-worker"
  "airflow/docker-compose.dev.yaml|"
  "service-main/docker-compose.dev.yaml|"
)

# host:port targets to validate once everything is up
targets=(
  "db:5432"
  "mongo:27017"
  "namenode:9000"
  "namenode:9870"
  "datanode:9864"
  "spark:7077"
  "spark:8080"
  "spark-worker:8081"
  "airflow:8080"
  "app:8000"
)

echo "--- Ensuring shared network '$network' exists"
docker network inspect "$network" >/dev/null 2>&1 || docker network create "$network"

for entry in "${stacks[@]}"; do
  stack="${entry%%|*}"
  services="${entry#*|}"
  echo "--- docker compose -f $stack up -d --build $services"
  docker compose -f "$root/$stack" up -d --build $services
done

echo "--- Waiting for services to settle..."
sleep 25

echo "--- Containers:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo
echo "--- Validating container connectivity on '$network'"
for t in "${targets[@]}"; do
  host="${t%%:*}"
  port="${t##*:}"
  if docker run --rm --network "$network" busybox sh -c "nc -z -w 2 $host $port" >/dev/null 2>&1; then
    echo "[OK]   $t"
  else
    echo "[FAIL] $t"
  fi
done

echo
echo "--- Optional extras (not started by this script):"
echo "  ./start-admin-uis.sh   -> adminer, mongo-express, dbt-docs"
echo "  ./start-streaming.sh   -> kafka, the booking/flight Structured Streaming app"
