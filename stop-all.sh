#!/usr/bin/env bash

set -e

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
network="ktdl-network"

stacks=(
  "dbt/docker-compose.dev.yaml"
  "service-main/docker-compose.dev.yaml"
  "airflow/docker-compose.dev.yaml"
  "spark/docker-compose.dev.yaml"
  "hadoop/docker-compose.dev.yaml"
  # "kafka/docker-compose.dev.yaml" # kafka disabled: streaming path out of scope
  "mongo/docker-compose.dev.yaml"
  "postgres/docker-compose.dev.yaml"
)

for stack in "${stacks[@]}"; do
  echo "--- docker compose -f $stack down"
  docker compose -f "$root/$stack" down
done

echo "--- Removing shared network '$network'"
docker network rm "$network" >/dev/null 2>&1 || true
