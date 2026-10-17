#!/usr/bin/env bash

# Starts the admin/observability UIs on demand. These aren't part of the
# pipeline itself - only run them while you're actively poking around, then
# stop them to free up resources:
#   docker compose -f postgres/docker-compose.dev.yaml stop adminer
#   docker compose -f mongo/docker-compose.dev.yaml stop mongo-express
#   docker compose -f dbt/docker-compose.dev.yaml stop dbt-docs
#
# Requires the core stack (./start-all.sh) already running - these attach to
# the existing db/mongo containers and the shared ktdl-network.

set -e

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "--- Starting adminer (Postgres admin UI)"
docker compose -f "$root/postgres/docker-compose.dev.yaml" up -d adminer

echo "--- Starting mongo-express (Mongo admin UI)"
docker compose -f "$root/mongo/docker-compose.dev.yaml" up -d mongo-express

echo "--- Starting dbt-docs (data lineage UI)"
echo "    Note: dbt-docs currently shows stale lineage - its models still point at"
echo "    the pre-refactor 8-table bronze schema, not the current 3-table one."
docker compose -f "$root/dbt/docker-compose.dev.yaml" up -d --build dbt-docs

echo
echo "--- Admin UIs:"
echo "  Adminer (Postgres):  http://localhost:8082"
echo "  Mongo Express:       http://localhost:8081"
echo "  dbt docs (lineage):  http://localhost:8085"
