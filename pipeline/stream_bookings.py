"""Streaming silver ingestion for booking events.

SQL lives in pipeline/sql/stream_bookings/, run in file order per Kafka
micro-batch - same pattern as pipeline/sql/silver/ for the batch path.
"""

import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common import ensure_namespaces, get_spark, run_sql_file

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka-0:9092")
TOPIC = os.environ.get("BOOKING_TOPIC", "booking.created")
CHECKPOINT_LOCATION = os.environ.get(
    "STREAM_CHECKPOINT",
    os.environ.get("LAKE_WAREHOUSE", "hdfs://namenode:9000/warehouse") + "/_checkpoints/booking_events",
)
TRIGGER_INTERVAL = os.environ.get("STREAM_TRIGGER_INTERVAL", "15 seconds")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://root:123456@mongo:27017/?authSource=admin")
MONGO_DB = os.environ.get("MONGO_DB", "airlines")

SQL_DIR = Path(__file__).resolve().parent / "sql" / "stream_bookings"

SEGMENT_SCHEMA = StructType(
    [
        StructField("ticket_no", StringType()),
        StructField("passenger_id", StringType()),
        StructField("passenger_name", StringType()),
        StructField("flight_id", IntegerType()),
        StructField("flight_no", StringType()),
        StructField("departure_airport", StringType()),
        StructField("arrival_airport", StringType()),
        StructField("aircraft_code", StringType()),
        StructField("scheduled_departure", TimestampType()),
        StructField("scheduled_arrival", TimestampType()),
        StructField("fare_conditions", StringType()),
        StructField("amount", DoubleType()),
    ]
)

EVENT_SCHEMA = StructType(
    [
        StructField("book_ref", StringType()),
        StructField("book_date", TimestampType()),
        StructField("total_amount", DoubleType()),
        StructField("segments", ArrayType(SEGMENT_SCHEMA)),
    ]
)


def write_live_metric(batch_df: DataFrame) -> None:
    """Per-real-minute booking counter in Mongo for the dashboard's live chart."""
    from datetime import datetime, timezone

    from pymongo import MongoClient

    agg = batch_df.agg(F.count(F.lit(1)).alias("n"), F.sum("total_amount").alias("rev")).first()
    n = int(agg["n"] or 0)
    if n == 0:
        return
    revenue = float(agg["rev"] or 0.0)
    minute_bucket = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        client[MONGO_DB]["live_bookings"].update_one(
            {"_id": minute_bucket.isoformat()},
            {"$set": {"minute": minute_bucket}, "$inc": {"bookings": n, "revenue": revenue}},
            upsert=True,
        )
    finally:
        client.close()


def process_batch(_spark: SparkSession, batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    spark = batch_df.sparkSession  # foreachBatch gives each batch its own session
    batch_id_str = f"stream_{batch_id}"
    batch_df = batch_df.withColumn("_ingest_ts", F.current_timestamp()).withColumn("_batch_id", F.lit(batch_id_str))
    batch_df.createOrReplaceTempView("booking_events_batch")

    for sql_file in sorted(SQL_DIR.glob("*.sql")):
        run_sql_file(spark, str(sql_file), batch_id=batch_id_str)

    try:
        write_live_metric(batch_df)
    except Exception:
        logger.exception("Failed to write live booking metric to Mongo")

    logger.info("Stream micro-batch %s processed (%d bookings)", batch_id, batch_df.count())


def main() -> None:
    spark = get_spark("airlines-stream-bookings")
    try:
        ensure_namespaces(spark)

        raw_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        parsed = raw_stream.select(
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data")
        ).select("data.*")

        query = (
            parsed.writeStream.foreachBatch(lambda batch_df, batch_id: process_batch(spark, batch_df, batch_id))
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )
        logger.info("Streaming query started, reading topic=%s", TOPIC)
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
