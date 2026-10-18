"""Runs the booking and flight streams as two queries in one Spark app.

One app instead of two spark-submit processes: a streaming query holds its
cores until stopped, and the worker only has 2.
"""

import logging
import sys
from pathlib import Path

# stream_bookings/stream_flights locate their own sql/ folder via __file__,
# which only resolves correctly if they're imported from this directory
# directly - not copied elsewhere by --py-files. Importing them this way
# keeps that path accurate.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import stream_bookings
import stream_flights
from pyspark.sql import functions as F

from common import ensure_namespaces, get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    spark = get_spark("airlines-stream")
    try:
        ensure_namespaces(spark)

        bookings_raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", stream_bookings.KAFKA_BOOTSTRAP)
            .option("subscribe", stream_bookings.TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        bookings_parsed = bookings_raw.select(
            F.from_json(F.col("value").cast("string"), stream_bookings.EVENT_SCHEMA).alias("data")
        ).select("data.*")
        bookings_parsed.writeStream.foreachBatch(
            lambda batch_df, batch_id: stream_bookings.process_batch(spark, batch_df, batch_id)
        ).option("checkpointLocation", stream_bookings.CHECKPOINT_LOCATION).trigger(
            processingTime=stream_bookings.TRIGGER_INTERVAL
        ).start()
        logger.info("Booking stream started, reading topic=%s", stream_bookings.TOPIC)

        flights_raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", stream_flights.KAFKA_BOOTSTRAP)
            .option("subscribe", f"{stream_flights.DEPARTED_TOPIC},{stream_flights.ARRIVED_TOPIC}")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        flights_parsed = flights_raw.select(
            F.from_json(F.col("value").cast("string"), stream_flights.EVENT_SCHEMA).alias("data")
        ).select("data.*")
        flights_parsed.writeStream.foreachBatch(
            lambda batch_df, batch_id: stream_flights.process_batch(spark, batch_df, batch_id)
        ).option("checkpointLocation", stream_flights.CHECKPOINT_LOCATION).trigger(
            processingTime=stream_flights.TRIGGER_INTERVAL
        ).start()
        logger.info(
            "Flight stream started, reading topics=%s,%s", stream_flights.DEPARTED_TOPIC, stream_flights.ARRIVED_TOPIC
        )

        spark.streams.awaitAnyTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
