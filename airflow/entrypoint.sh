#!/usr/bin/env bash
set -e

echo "Waiting for postgres database 'airflow' at db:5432..."
python3 -c '
import os, sys, time
import psycopg2

user = os.environ.get("POSTGRES_USER", "postgres")
password = os.environ.get("POSTGRES_PASSWORD", "123456")
host = os.environ.get("PG_HOST", "db")
port = int(os.environ.get("PG_PORT", "5432"))
dbname = "airflow"

for attempt in range(60):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
            connect_timeout=2,
        )
        conn.close()
        print("Postgres database \"airflow\" is ready.")
        sys.exit(0)
    except Exception:
        time.sleep(1)
print("Timed out waiting for database \"airflow\".", file=sys.stderr)
sys.exit(1)
'

echo "Running airflow db migrate..."
airflow db migrate

echo "Ensuring admin user exists..."
airflow users create \
  --username "${AIRFLOW_ADMIN_USERNAME:-admin}" \
  --firstname "${AIRFLOW_ADMIN_FIRSTNAME:-Admin}" \
  --lastname "${AIRFLOW_ADMIN_LASTNAME:-User}" \
  --role Admin \
  --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" \
  --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" || true

echo "Starting airflow standalone..."
exec airflow standalone
