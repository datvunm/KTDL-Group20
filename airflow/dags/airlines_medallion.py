from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


def sanitize_run_id(val: Any) -> str:
    """Sanitize run_id string to retain only [A-Za-z0-9_]."""
    return re.sub(r"[^A-Za-z0-9_]", "_", str(val))


RAW_SOURCE_SQL = os.environ.get("RAW_SOURCE_SQL", "/opt/pipeline/sql/bronze/airline_data_source.sql")
RAW_SCHEMA = "raw"


def pg_connect() -> Any:
    """Open a psycopg2 connection to the source demo database (PG_URL / PG_* env)."""
    import psycopg2

    pg_url = os.environ.get("PG_URL", "jdbc:postgresql://db:5432/demo")
    host = os.environ.get("PG_HOST", "db")
    port = int(os.environ.get("PG_PORT", "5432"))
    dbname = os.environ.get("PG_DATABASE", "demo")
    user = os.environ.get("PG_USER", "postgres")
    password = os.environ.get("PG_PASSWORD", "123456")

    if pg_url.startswith("jdbc:postgresql://"):
        raw = pg_url.replace("jdbc:postgresql://", "")
        if "/" in raw:
            hp, db = raw.split("/", 1)
            dbname = db.split("?")[0]
            if ":" in hp:
                host, p = hp.split(":", 1)
                port = int(p)
            else:
                host = hp

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        connect_timeout=10,
    )


def check_source_callable(**context: Any) -> str:
    """Verify source database demo: select bookings.now() and verify bookings row count."""
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT bookings.now();")
            row = cur.fetchone()
            cutoff = row[0] if row else None

            cur.execute("SELECT count(*) FROM bookings.bookings;")
            cnt_row = cur.fetchone()
            cnt = cnt_row[0] if cnt_row else 0
            if cnt == 0:
                raise ValueError("bookings.bookings is empty; simulator has not run or cutoff empty")

            cutoff_str = cutoff.isoformat() if hasattr(cutoff, "isoformat") else str(cutoff)
            ti = context.get("ti")
            if ti:
                ti.xcom_push(key="cutoff", value=cutoff_str)
            return cutoff_str
    finally:
        conn.close()


def split_sql_statements(content: str) -> list[str]:
    """Split a SQL script into statements on lines ending with ';', dropping '--' comment lines."""
    statements = []
    current: list[str] = []
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(current).strip().rstrip(";").strip())
            current = []
    if current:
        statements.append("\n".join(current).strip().rstrip(";").strip())
    return [s for s in statements if s]


def check_raw_validation(columns: list[str], rows: list[tuple]) -> None:
    """Assert one validation result set from the raw source script.

    Supports the script's checks: segment row count, lossless reference
    copies, missing keys, and (optionally) the per-table recovery test.
    """
    if columns[:3] == ["table_name", "lost", "extra"]:
        bad = [r for r in rows if r[1] != 0 or r[2] != 0]
        if bad:
            raise ValueError(f"Raw recovery test failed (table, lost, extra): {bad}")
        return

    values = dict(zip(columns, rows[0]))
    if "booked_rows" in values:
        if values["booked_rows"] != values["ticket_flights"]:
            raise ValueError(f"Raw booked rows do not match ticket_flights: {values}")
    elif "airports_raw" in values:
        if values["airports_raw"] != values["airports_src"] or values["seats_raw"] != values["seats_src"]:
            raise ValueError(f"Raw reference tables are not lossless copies: {values}")
    elif all(c.startswith("missing_") for c in columns):
        if any(v != 0 for v in values.values()):
            raise ValueError(f"Raw booking table has unresolved reference keys: {values}")
    else:
        raise ValueError(f"Unrecognized raw validation query result: {columns}")


def prepare_raw_source_callable(**context: Any) -> dict[str, int]:
    """Rebuild the raw.* source tables from the simulated bookings schema.

    Runs the DDL in airline_data_source.sql with search_path (raw, bookings)
    so tables land in schema raw and read the current simulation state.
    Validation queries must pass; the heavy recovery test (a WITH query)
    only runs when RAW_RECOVERY_CHECK=1. Everything commits in one transaction.
    """
    statements = split_sql_statements(Path(RAW_SOURCE_SQL).read_text(encoding="utf-8"))
    run_recovery = os.environ.get("RAW_RECOVERY_CHECK", "0") == "1"

    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
            cur.execute(f"SET LOCAL search_path = {RAW_SCHEMA}, bookings")
            for stmt in statements:
                is_recovery = stmt.upper().startswith("WITH")
                if is_recovery and not run_recovery:
                    print("Skipping raw recovery test (set RAW_RECOVERY_CHECK=1 to enable)")
                    continue
                print(f"Executing: {stmt.splitlines()[0]} ...")
                cur.execute(stmt)
                if cur.description is not None:
                    columns = [d[0] for d in cur.description]
                    rows = cur.fetchall()
                    print(f"  {columns} -> {rows}")
                    check_raw_validation(columns, rows)

            # Bronze reads flight_seat_reservations in flight_id ranges
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS flight_seat_reservations_flight_id_idx "
                f"ON {RAW_SCHEMA}.flight_seat_reservations (flight_id)"
            )
            counts = {}
            for table in ["flight_seat_reservations", "airport_sites", "aircraft_seat_layouts"]:
                cur.execute(f"SELECT count(*) FROM {RAW_SCHEMA}.{table}")
                counts[table] = cur.fetchone()[0]
        conn.commit()
        print(f"Raw source tables rebuilt: {counts}")
        return counts
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def on_failure_callback(context: Any) -> None:
    """DAG-level failure callback: upsert a pipeline_runs doc with status failed in MongoDB."""
    try:
        import pymongo

        dag_run = context.get("dag_run")
        raw_run_id = context.get("run_id") or (dag_run.run_id if dag_run else "unknown")
        run_id = sanitize_run_id(raw_run_id)

        mongo_uri = os.environ.get("MONGO_URI", "mongodb://root:123456@mongo:27017/?authSource=admin")
        mongo_db = os.environ.get("MONGO_DB", "airlines")

        ti = context.get("ti")
        cutoff = None
        if ti:
            cutoff = ti.xcom_pull(task_ids="check_source", key="cutoff")

        started_at = None
        if dag_run and dag_run.start_date:
            started_at = dag_run.start_date.isoformat()

        finished_at = datetime.now(timezone.utc).isoformat()

        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        db = client[mongo_db]
        db.pipeline_runs.update_one(
            {"_id": run_id},
            {
                "$set": {
                    "_id": run_id,
                    "run_id": run_id,
                    "cutoff": cutoff,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "status": "failed",
                }
            },
            upsert=True,
        )
        client.close()
    except Exception as exc:
        print(f"Warning: Failed to record failure in MongoDB: {exc}")


spark_conf = {
    "spark.driver.host": "airflow",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.port": "7078",
    "spark.blockManager.port": "7079",
    "spark.driver.memory": "512m",
    "spark.executor.memory": "1g",
    "spark.executor.cores": "2",
    "spark.cores.max": "2",
    "spark.sql.shuffle.partitions": "8",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.session.timeZone": "UTC",
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.mongodb.spark:mongo-spark-connector_2.12:10.4.1,org.postgresql:postgresql:42.7.4",
    "spark.jars.ivy": "/opt/airflow/.ivy2",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lake": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lake.type": "hadoop",
    "spark.sql.catalog.lake.warehouse": os.environ.get("LAKE_WAREHOUSE", "hdfs://namenode:9000/warehouse"),
    "spark.executorEnv.HADOOP_USER_NAME": "root",
}

pipeline_env = {
    "PG_URL": os.environ.get("PG_URL", "jdbc:postgresql://db:5432/demo"),
    "PG_USER": os.environ.get("PG_USER", "postgres"),
    "PG_PASSWORD": os.environ.get("PG_PASSWORD", "123456"),
    "LAKE_WAREHOUSE": os.environ.get("LAKE_WAREHOUSE", "hdfs://namenode:9000/warehouse"),
    "MONGO_URI": os.environ.get("MONGO_URI", "mongodb://root:123456@mongo:27017/?authSource=admin"),
    "MONGO_DB": os.environ.get("MONGO_DB", "airlines"),
    "HADOOP_USER_NAME": "root",
}

with DAG(
    dag_id="airlines_medallion",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=on_failure_callback,
    user_defined_filters={"sanitize_run_id": sanitize_run_id},
    user_defined_macros={"sanitize_run_id": sanitize_run_id},
    tags=["ktdl", "lakehouse", "airlines"],
) as dag:
    check_source = PythonOperator(
        task_id="check_source",
        python_callable=check_source_callable,
    )

    prepare_raw_source = PythonOperator(
        task_id="prepare_raw_source",
        python_callable=prepare_raw_source_callable,
    )

    bronze = SparkSubmitOperator(
        task_id="bronze",
        conn_id="spark_default",
        application="/opt/pipeline/bronze.py",
        py_files="/opt/pipeline/common.py",
        application_args=["--run-id", "{{ run_id | sanitize_run_id }}"],
        conf=spark_conf,
        env_vars=pipeline_env,
    )

    silver = SparkSubmitOperator(
        task_id="silver",
        conn_id="spark_default",
        application="/opt/pipeline/silver.py",
        py_files="/opt/pipeline/common.py",
        application_args=["--run-id", "{{ run_id | sanitize_run_id }}"],
        conf=spark_conf,
        env_vars=pipeline_env,
    )

    gold = SparkSubmitOperator(
        task_id="gold",
        conn_id="spark_default",
        application="/opt/pipeline/gold.py",
        py_files="/opt/pipeline/common.py",
        application_args=["--run-id", "{{ run_id | sanitize_run_id }}"],
        conf=spark_conf,
        env_vars=pipeline_env,
    )

    publish = SparkSubmitOperator(
        task_id="publish",
        conn_id="spark_default",
        application="/opt/pipeline/publish.py",
        py_files="/opt/pipeline/common.py",
        application_args=["--run-id", "{{ run_id | sanitize_run_id }}"],
        conf=spark_conf,
        env_vars=pipeline_env,
    )

    check_source >> prepare_raw_source >> bronze >> silver >> gold >> publish
