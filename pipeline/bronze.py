"""Bronze ingestion layer for Airlines Lakehouse.

Reads tables from PostgreSQL demo schema via JDBC and appends to Iceberg
lake.bronze.* tables partitioned by days(_ingest_ts).
Tracks incremental watermarks in lake.meta.watermarks.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import ensure_namespaces, get_spark, parse_args, read_pg

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_WATERMARK = "1900-01-01 00:00:00+00"


def init_watermark_table(spark: SparkSession) -> None:
    """Ensure lake.meta.watermarks table exists."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lake.meta.watermarks (
            table_name string,
            watermark timestamp,
            run_id string,
            updated_at timestamp
        ) USING iceberg
    """)


def get_watermark(spark: SparkSession, table_name: str) -> str:
    """Fetch the latest watermark for table_name or return the 1900 default."""
    try:
        rows = (
            spark.sql(f"SELECT watermark FROM lake.meta.watermarks WHERE table_name = '{table_name}'")
            .collect()
        )
        if rows and rows[0]["watermark"] is not None:
            wm_val = rows[0]["watermark"]
            if hasattr(wm_val, "strftime"):
                return f"{wm_val.strftime('%Y-%m-%d %H:%M:%S')}+00"
            s = str(wm_val)
            return s if s.endswith("+00") or s.endswith("Z") else f"{s}+00"
    except Exception as e:
        logger.warning("Could not read watermark for %s: %s; using default", table_name, e)
    return DEFAULT_WATERMARK


def update_watermark(spark: SparkSession, table_name: str, cutoff_str: str, run_id: str) -> None:
    """Update watermark for table_name to cutoff_str using Iceberg MERGE."""
    spark.sql(f"""
        MERGE INTO lake.meta.watermarks AS target
        USING (
            SELECT
                '{table_name}' AS table_name,
                TIMESTAMP '{cutoff_str}' AS watermark,
                '{run_id}' AS run_id,
                current_timestamp() AS updated_at
        ) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET
            target.watermark = source.watermark,
            target.run_id = source.run_id,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)


def write_bronze(spark: SparkSession, df: DataFrame, table_name: str) -> None:
    """Append DataFrame to lake.bronze.table_name, creating table if needed."""
    full_table = f"lake.bronze.{table_name}"
    if spark.catalog.tableExists(full_table):
        logger.info("Appending to existing table %s", full_table)
        df.writeTo(full_table).append()
    else:
        logger.info("Creating new partitioned Iceberg table %s", full_table)
        df.writeTo(full_table).partitionedBy(F.days(F.col("_ingest_ts"))).create()


def add_metadata(df: DataFrame, run_id: str, cutoff_str: str) -> DataFrame:
    """Append bronze metadata columns: _ingest_ts, _batch_id, _source_now."""
    return (
        df.withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_batch_id", F.lit(run_id))
        .withColumn("_source_now", F.to_timestamp(F.lit(cutoff_str)))
    )


def run_bronze(spark: SparkSession, run_id: str) -> None:
    """Execute bronze batch ingestion."""
    ensure_namespaces(spark)
    init_watermark_table(spark)

    # 1. Fetch simulation cutoff timestamp from Postgres
    cutoff_df = read_pg(spark, "SELECT bookings.now() AS cutoff")
    cutoff_val = cutoff_df.collect()[0]["cutoff"]
    if hasattr(cutoff_val, "strftime"):
        cutoff_str = f"{cutoff_val.strftime('%Y-%m-%d %H:%M:%S')}+00"
    else:
        s = str(cutoff_val)
        cutoff_str = s if s.endswith("+00") or s.endswith("Z") else f"{s}+00"
    logger.info("Ingesting bronze batch %s with cutoff %s", run_id, cutoff_str)

    # 2. Ingest dimensional / full tables: aircrafts_data, airports_data, seats
    logger.info("Loading aircrafts_data...")
    df_aircrafts = read_pg(
        spark,
        "SELECT aircraft_code, model::text AS model, range FROM bookings.aircrafts_data",
    )
    write_bronze(spark, add_metadata(df_aircrafts, run_id, cutoff_str), "aircrafts_data")

    logger.info("Loading airports_data...")
    df_airports = read_pg(
        spark,
        "SELECT airport_code, airport_name::text AS airport_name, city::text AS city, "
        "coordinates::text AS coordinates, timezone FROM bookings.airports_data",
    )
    write_bronze(spark, add_metadata(df_airports, run_id, cutoff_str), "airports_data")

    logger.info("Loading seats...")
    df_seats = read_pg(
        spark,
        "SELECT aircraft_code, seat_no, fare_conditions FROM bookings.seats",
    )
    write_bronze(spark, add_metadata(df_seats, run_id, cutoff_str), "seats")

    # 3. Ingest flights: full snapshot each run with 4-partition JDBC read
    logger.info("Loading flights snapshot...")
    bounds_df = read_pg(
        spark,
        "SELECT min(flight_id) AS min_id, max(flight_id) AS max_id FROM bookings.flights",
    )
    bounds = bounds_df.collect()[0]
    min_id, max_id = bounds["min_id"], bounds["max_id"]
    flights_sql = (
        "SELECT flight_id, flight_no, scheduled_departure, scheduled_arrival, "
        "departure_airport, arrival_airport, status, aircraft_code, actual_departure, actual_arrival "
        "FROM bookings.flights"
    )
    if min_id is not None and max_id is not None and min_id < max_id:
        df_flights = read_pg(
            spark,
            flights_sql,
            partition_column="flight_id",
            lower=min_id,
            upper=max_id,
            num_partitions=4,
        )
    else:
        df_flights = read_pg(spark, flights_sql)
    write_bronze(spark, add_metadata(df_flights, run_id, cutoff_str), "flights")

    # 4. Ingest bookings: incremental window (wm, cutoff]
    wm_bookings = get_watermark(spark, "bookings")
    logger.info("Loading bookings with window (%s, %s]...", wm_bookings, cutoff_str)
    df_bookings = read_pg(
        spark,
        f"SELECT book_ref, book_date, total_amount FROM bookings.bookings "
        f"WHERE book_date > '{wm_bookings}' AND book_date <= '{cutoff_str}'",
    )
    write_bronze(spark, add_metadata(df_bookings, run_id, cutoff_str), "bookings")

    # 5. Ingest tickets: join bookings on same window
    logger.info("Loading tickets...")
    df_tickets = read_pg(
        spark,
        f"SELECT t.ticket_no, t.book_ref, t.passenger_id, t.passenger_name, "
        f"t.contact_data::text AS contact_data "
        f"FROM bookings.tickets t "
        f"JOIN bookings.bookings b ON t.book_ref = b.book_ref "
        f"WHERE b.book_date > '{wm_bookings}' AND b.book_date <= '{cutoff_str}'",
    )
    write_bronze(spark, add_metadata(df_tickets, run_id, cutoff_str), "tickets")

    # 6. Ingest ticket_flights: join tickets -> bookings on same window
    logger.info("Loading ticket_flights...")
    df_ticket_flights = read_pg(
        spark,
        f"SELECT tf.ticket_no, tf.flight_id, tf.fare_conditions, tf.amount "
        f"FROM bookings.ticket_flights tf "
        f"JOIN bookings.tickets t ON tf.ticket_no = t.ticket_no "
        f"JOIN bookings.bookings b ON t.book_ref = b.book_ref "
        f"WHERE b.book_date > '{wm_bookings}' AND b.book_date <= '{cutoff_str}'",
    )
    write_bronze(spark, add_metadata(df_ticket_flights, run_id, cutoff_str), "ticket_flights")

    # 7. Ingest boarding_passes: own watermark on release time
    wm_boarding = get_watermark(spark, "boarding_passes")
    logger.info("Loading boarding_passes with window (%s, %s]...", wm_boarding, cutoff_str)
    df_boarding_passes = read_pg(
        spark,
        f"SELECT bp.ticket_no, bp.flight_id, bp.boarding_no, bp.seat_no "
        f"FROM bookings.boarding_passes bp "
        f"JOIN bookings.tickets t ON bp.ticket_no = t.ticket_no "
        f"JOIN bookings.bookings b ON t.book_ref = b.book_ref "
        f"JOIN bookings.flights f ON bp.flight_id = f.flight_id "
        f"WHERE greatest(f.scheduled_departure - interval '24 hours', b.book_date) > '{wm_boarding}' "
        f"  AND greatest(f.scheduled_departure - interval '24 hours', b.book_date) <= '{cutoff_str}'",
    )
    write_bronze(spark, add_metadata(df_boarding_passes, run_id, cutoff_str), "boarding_passes")

    # 8. Update watermarks only after all loads succeed
    logger.info("Updating watermarks to %s for run %s", cutoff_str, run_id)
    update_watermark(spark, "bookings", cutoff_str, run_id)
    update_watermark(spark, "tickets", cutoff_str, run_id)
    update_watermark(spark, "ticket_flights", cutoff_str, run_id)
    update_watermark(spark, "boarding_passes", cutoff_str, run_id)
    update_watermark(spark, "aircrafts_data", cutoff_str, run_id)
    update_watermark(spark, "airports_data", cutoff_str, run_id)
    update_watermark(spark, "seats", cutoff_str, run_id)
    update_watermark(spark, "flights", cutoff_str, run_id)
    logger.info("Bronze ingestion batch %s completed successfully", run_id)


def main() -> None:
    args = parse_args("Bronze Ingestion Job")
    spark = get_spark("airlines-bronze")
    try:
        run_bronze(spark, args.run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
