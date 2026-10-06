from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


def sanitize_run_id(val: Any) -> str:
    """Sanitize run_id string to retain only [A-Za-z0-9_]."""
    return re.sub(r"[^A-Za-z0-9_]", "_", str(val))


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
    """Verify source database demo: select raw.now() and verify raw has booked rows."""
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT raw.now();")
            row = cur.fetchone()
            cutoff = row[0] if row else None

            cur.execute("SELECT EXISTS (SELECT 1 FROM raw.flight_seat_reservations WHERE ticket_no IS NOT NULL);")
            has_booked = cur.fetchone()[0]
            if not has_booked:
                raise ValueError(
                    "raw.flight_seat_reservations has no booked rows; simulator has not run or cutoff empty"
                )

            cutoff_str = cutoff.isoformat() if hasattr(cutoff, "isoformat") else str(cutoff)
            ti = context.get("ti")
            if ti:
                ti.xcom_push(key="cutoff", value=cutoff_str)
            return cutoff_str
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

    check_source >> bronze >> silver >> gold >> publish
