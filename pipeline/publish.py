"""Publishing layer for Airlines Lakehouse.

Writes Gold Iceberg marts to MongoDB collections using the mongo-spark connector:
- gold_route_revenue
- gold_route_pareto
- gold_flight_occupancy
- gold_fleet
- gold_delay_heatmap
- gold_delay_by_aircraft
- gold_delay_by_route
- dim_airports
- dim_routes

Performs post-publish driver cleanup via PyMongo:
- Purges records from prior runs (_run_id != run_id)
- Ensures indexes on month, dep_airport, aircraft_code where applicable
- Builds and records metadata document into pipeline_runs collection
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import get_spark, parse_args

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

GOLD_TABLES = [
    "gold_route_revenue",
    "gold_route_pareto",
    "gold_flight_occupancy",
    "gold_fleet",
    "gold_delay_heatmap",
    "gold_delay_by_aircraft",
    "gold_delay_by_route",
    "dim_airports",
    "dim_routes",
]

BRONZE_TABLES = [
    "aircraft_seat_layouts",
    "airport_sites",
    "flight_seat_reservations",
]

SILVER_TABLES = [
    "airports",
    "aircrafts",
    "seats",
    "bookings",
    "tickets",
    "flights_enriched",
    "ticket_flights",
    "boarding_passes",
]

INDEX_SPECS = {
    "month": ["gold_route_revenue", "gold_flight_occupancy"],
    "dep_airport": [
        "gold_route_revenue",
        "gold_route_pareto",
        "gold_flight_occupancy",
        "gold_delay_by_route",
        "dim_routes",
    ],
    "aircraft_code": [
        "gold_flight_occupancy",
        "gold_fleet",
        "gold_delay_by_aircraft",
    ],
}


def clean_old_runs(db: Any, collection_names: List[str], current_run_id: str) -> Dict[str, int]:
    """Delete documents from previous runs across specified collections."""
    deleted_counts = {}
    for col_name in collection_names:
        collection = db[col_name]
        res = collection.delete_many({"_run_id": {"$ne": current_run_id}})
        deleted_counts[col_name] = res.deleted_count
        logger.info("Cleaned %d stale records from Mongo collection %s", res.deleted_count, col_name)
    return deleted_counts


def create_indexes(db: Any, collection_names: Optional[List[str]] = None) -> None:
    """Ensure indexes on month, dep_airport, and aircraft_code where appropriate."""
    active_cols = set(collection_names) if collection_names is not None else set(GOLD_TABLES)

    for field, candidate_cols in INDEX_SPECS.items():
        for col_name in candidate_cols:
            if col_name in active_cols:
                logger.info("Creating index on %s(%s)...", col_name, field)
                db[col_name].create_index([(field, 1)])


def _to_iso(ts: Any) -> str:
    """Helper to convert timestamp or datetime to ISO format string."""
    if ts is None:
        return datetime.now(timezone.utc).isoformat()
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.isoformat()
    if hasattr(ts, "isoformat"):
        return ts.isoformat()
    s = str(ts).strip()
    try:
        dt = datetime.fromisoformat(s.replace(" ", "T"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return s


def build_pipeline_runs_doc(
    run_id: str,
    cutoff: Any,
    started_at: Any,
    finished_at: Any,
    status: str,
    counts: Dict[str, Dict[str, int]],
    quarantine: int,
) -> Dict[str, Any]:
    """Construct pipeline_runs summary document matching contract specification."""
    return {
        "_id": run_id,
        "run_id": run_id,
        "cutoff": _to_iso(cutoff),
        "started_at": _to_iso(started_at),
        "finished_at": _to_iso(finished_at),
        "status": status,
        "counts": counts,
        "quarantine": int(quarantine),
    }


def record_pipeline_run(db: Any, run_doc: Dict[str, Any]) -> None:
    """Upsert pipeline run status document into pipeline_runs collection."""
    logger.info("Recording pipeline_runs doc for run_id=%s with status=%s", run_doc["run_id"], run_doc["status"])
    db["pipeline_runs"].replace_one({"_id": run_doc["_id"]}, run_doc, upsert=True)


def collect_metrics(spark: SparkSession, run_id: str) -> Dict[str, Any]:
    """Collect row counts across Bronze, Silver, Gold, and Quarantine."""
    counts: Dict[str, Dict[str, int]] = {"bronze": {}, "silver": {}, "gold": {}}

    for t in BRONZE_TABLES:
        try:
            cnt = spark.sql(f"SELECT count(*) FROM lake.bronze.{t} WHERE _batch_id = '{run_id}'").collect()[0][0]
            counts["bronze"][t] = int(cnt)
        except Exception:
            counts["bronze"][t] = 0

    for t in SILVER_TABLES:
        try:
            cnt = spark.sql(f"SELECT count(*) FROM lake.silver.{t}").collect()[0][0]
            counts["silver"][t] = int(cnt)
        except Exception:
            counts["silver"][t] = 0

    for t in GOLD_TABLES:
        try:
            cnt = spark.sql(f"SELECT count(*) FROM lake.gold.{t}").collect()[0][0]
            counts["gold"][t] = int(cnt)
        except Exception:
            counts["gold"][t] = 0

    quarantine_cnt = 0
    try:
        q_row = spark.sql(f"SELECT count(*) FROM lake.silver.quarantine WHERE _batch_id = '{run_id}'").collect()
        if q_row:
            quarantine_cnt = int(q_row[0][0])
    except Exception:
        quarantine_cnt = 0

    # Query timing from bronze metadata
    started_at = None
    cutoff = None
    try:
        timing_row = spark.sql(
            f"SELECT min(_ingest_ts) AS started_at, max(_source_now) AS cutoff "
            f"FROM lake.bronze.flight_seat_reservations WHERE _batch_id = '{run_id}'"
        ).collect()
        if timing_row:
            started_at = timing_row[0]["started_at"]
            cutoff = timing_row[0]["cutoff"]
    except Exception:
        pass

    if started_at is None:
        started_at = datetime.now(timezone.utc)
    if cutoff is None:
        cutoff = datetime.now(timezone.utc)

    return {
        "counts": counts,
        "quarantine": quarantine_cnt,
        "started_at": started_at,
        "cutoff": cutoff,
    }


def publish_marts_to_mongo(
    spark: SparkSession,
    run_id: str,
    mongo_uri: str,
    mongo_db: str,
    marts: List[str] = GOLD_TABLES,
) -> None:
    """Write gold marts to MongoDB collections using Mongo Spark Connector."""
    for mart in marts:
        full_table = f"lake.gold.{mart}"
        logger.info("Publishing %s to MongoDB collection %s...", full_table, mart)
        df = spark.table(full_table).withColumn("_run_id", F.lit(run_id))
        (
            df.write.format("mongodb")
            .mode("append")
            .option("connection.uri", mongo_uri)
            .option("database", mongo_db)
            .option("collection", mart)
            .option("operationType", "replace")
            .option("idFieldList", "_id")
            .option("upsertDocument", "true")
            .save()
        )


def run_publish(spark: SparkSession, run_id: str) -> None:
    """Execute full publish stage."""
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://root:123456@mongo:27017/?authSource=admin")
    mongo_db = os.environ.get("MONGO_DB", "airlines")

    # 1. Write gold marts to MongoDB with _run_id
    publish_marts_to_mongo(spark, run_id, mongo_uri, mongo_db, GOLD_TABLES)

    # 2. PyMongo maintenance on driver
    metrics = collect_metrics(spark, run_id)
    finished_at = datetime.now(timezone.utc)

    client: MongoClient = MongoClient(mongo_uri)
    try:
        db = client[mongo_db]
        clean_old_runs(db, GOLD_TABLES, run_id)
        create_indexes(db, GOLD_TABLES)

        run_doc = build_pipeline_runs_doc(
            run_id=run_id,
            cutoff=metrics["cutoff"],
            started_at=metrics["started_at"],
            finished_at=finished_at,
            status="success",
            counts=metrics["counts"],
            quarantine=metrics["quarantine"],
        )
        record_pipeline_run(db, run_doc)
        logger.info("Publish completed successfully for run_id=%s", run_id)
    finally:
        client.close()


def main() -> None:
    args = parse_args("Gold Marts Publish Job")
    spark = get_spark("airlines-publish")
    try:
        run_publish(spark, args.run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
