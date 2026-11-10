#!/usr/bin/env bash

# Stops everything and deletes this project's data volumes - Postgres (both
# demo and Airflow's own metadata db), Mongo, HDFS, Kafka. Back to a blank
# slate. Re-init with:
#   ./start-all.sh
#   ./source/load_dump.sh
#
# Airflow's own metadata db (DAG run history etc.) gets wiped along with
# demo - its admin user is recreated automatically on next start with the
# fixed admin/admin default (see airflow/entrypoint.sh), nothing to recover.
#
# Only removes volumes this project's own compose files define - never a
# blanket `docker volume prune` (other projects' volumes live on this
# machine too).

set -e

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
network="ktdl-network"

volumes=(
  postgres_postgres_data
  mongo_mongo_data
  hadoop_hadoop_namenode
  hadoop_hadoop_datanode
  kafka_kafka_0_data
  kafka_kafka_1_data  # orphaned, from the 3-broker config before the single-broker trim
  kafka_kafka_2_data  # orphaned, same
)

stacks=(
  "dbt/docker-compose.dev.yaml"
  "service-main/docker-compose.dev.yaml"
  "airflow/docker-compose.dev.yaml"
  "kafka/docker-compose.dev.yaml"
  "spark/docker-compose.dev.yaml"
  "hadoop/docker-compose.dev.yaml"
  "mongo/docker-compose.dev.yaml"
  "postgres/docker-compose.dev.yaml"
)

echo "This will stop every container and permanently delete:"
for v in "${volumes[@]}"; do
  echo "  - $v"
done
echo
read -r -p "Type 'reset' to confirm: " confirm
if [ "$confirm" != "reset" ]; then
  echo "Aborted - nothing changed."
  exit 1
fi

echo "--- Stopping all stacks"
for stack in "${stacks[@]}"; do
  docker compose -f "$root/$stack" down
done

echo "--- Removing data volumes"
for v in "${volumes[@]}"; do
  docker volume rm "$v" 2>/dev/null || echo "  (already gone: $v)"
done

echo "--- Removing shared network '$network'"
docker network rm "$network" >/dev/null 2>&1 || true

echo
echo "Done. Re-init with:"
echo "  ./start-all.sh"
echo "  ./source/load_dump.sh"
