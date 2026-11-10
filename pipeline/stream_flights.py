"""Streaming flight status for the Airlines Lakehouse.

SQL lives in pipeline/sql/stream_flights/, run in file order per Kafka
micro-batch - same pattern as pipeline/sql/silver/ for the batch path.
Unlike stream_bookings.py, this is allowed to update status/actuals: these
are real recorded departure/arrival timestamps, not placeholders.
"""

import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from common import ensure_namespaces, get_spark, run_sql_file

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka-0:9092")
DEPARTED_TOPIC = os.environ.get("DEPARTED_TOPIC", "flight.departed")
ARRIVED_TOPIC = os.environ.get("ARRIVED_TOPIC", "flight.arrived")
CHECKPOINT_LOCATION = os.environ.get(
    "FLIGHT_STREAM_CHECKPOINT",
    os.environ.get("LAKE_WAREHOUSE", "hdfs://namenode:9000/warehouse") + "/_checkpoints/flight_events",
)
TRIGGER_INTERVAL = os.environ.get("STREAM_TRIGGER_INTERVAL", "15 seconds")

SQL_DIR = Path(__file__).resolve().parent / "sql" / "stream_flights"

EVENT_SCHEMA = StructType(
    [
        StructField("flight_id", IntegerType()),
        StructField("flight_no", StringType()),
        StructField("scheduled_departure", TimestampType()),
        StructField("scheduled_arrival", TimestampType()),
        StructField("departure_airport", StringType()),
        StructField("arrival_airport", StringType()),
        StructField("aircraft_code", StringType()),
        StructField("status", StringType()),
        StructField("actual_departure", TimestampType()),
        StructField("actual_arrival", TimestampType()),
    ]
)


def process_batch(_spark: SparkSession, batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    spark = batch_df.sparkSession
    batch_id_str = f"stream_{batch_id}"
    batch_df = batch_df.withColumn("_ingest_ts", F.current_timestamp()).withColumn("_batch_id", F.lit(batch_id_str))
    batch_df.createOrReplaceTempView("flight_events_batch")

    for sql_file in sorted(SQL_DIR.glob("*.sql")):
        run_sql_file(spark, str(sql_file), batch_id=batch_id_str)

    logger.info("Flight-events micro-batch %s processed (%d events)", batch_id, batch_df.count())


def main() -> None:
    spark = get_spark("airlines-stream-flights")
    try:
        ensure_namespaces(spark)

        raw_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", f"{DEPARTED_TOPIC},{ARRIVED_TOPIC}")
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
        logger.info(
            "Streaming query started, reading topics=%s,%s", DEPARTED_TOPIC, ARRIVED_TOPIC
        )
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
