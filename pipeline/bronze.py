"""Bronze ingestion layer for Airlines Lakehouse.

Reads the three denormalized raw source tables (schema raw in the demo DB,
rebuilt by the Airflow task prepare_raw_source from
sql/bronze/airline_data_source.sql) via JDBC and appends them as-is to
Iceberg lake.bronze.* tables partitioned by days(_ingest_ts) and _batch_id:

- flight_seat_reservations : one row per seat per flight + unseated bookings
- airport_sites            : airport reference
- aircraft_seat_layouts    : aircraft model + seat map reference

Every run takes a full snapshot (the raw grain has no change-tracking
column, and seat/status rows change between cutoffs). The cutoff of each
load is recorded in lake.meta.watermarks.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import ensure_namespaces, get_spark, parse_args, read_pg

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RAW_SCHEMA = "raw"
BRONZE_TABLES = ["aircraft_seat_layouts", "airport_sites", "flight_seat_reservations"]
FSR_PARTITIONS = 8


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
        df.writeTo(full_table).partitionedBy(F.days(F.col("_ingest_ts")), F.col("_batch_id")).create()


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

    # 2. Reference tables: full copy of airport_sites and aircraft_seat_layouts
    logger.info("Loading %s.aircraft_seat_layouts...", RAW_SCHEMA)
    df_layouts = read_pg(
        spark,
        f"SELECT aircraft_code, model::text AS model, range, seat_no, seats_fare_conditions "
        f"FROM {RAW_SCHEMA}.aircraft_seat_layouts",
    )
    write_bronze(spark, add_metadata(df_layouts, run_id, cutoff_str), "aircraft_seat_layouts")

    logger.info("Loading %s.airport_sites...", RAW_SCHEMA)
    df_airports = read_pg(
        spark,
        f"SELECT airport_code, airport_name::text AS airport_name, city::text AS city, "
        f"coordinates::text AS coordinates, timezone FROM {RAW_SCHEMA}.airport_sites",
    )
    write_bronze(spark, add_metadata(df_airports, run_id, cutoff_str), "airport_sites")

    # 3. flight_seat_reservations: full snapshot with partitioned JDBC read on flight_id.
    # jsonb / int[] / interval are cast to text to keep the raw values as-is.
    logger.info("Loading %s.flight_seat_reservations snapshot...", RAW_SCHEMA)
    bounds = read_pg(
        spark,
        f"SELECT min(flight_id) AS min_id, max(flight_id) AS max_id FROM {RAW_SCHEMA}.flight_seat_reservations",
    ).collect()[0]
    min_id, max_id = bounds["min_id"], bounds["max_id"]
    fsr_sql = (
        "SELECT flight_id, flight_no, status, scheduled_departure, scheduled_arrival, "
        "actual_departure, actual_arrival, departure_airport, arrival_airport, aircraft_code, "
        "days_of_week::text AS days_of_week, duration::text AS duration, "
        "seat_no, boarding_no, "
        "ticket_no, ticket_flights_fare_conditions, amount, "
        "book_ref, passenger_id, passenger_name, contact_data::text AS contact_data, "
        "book_date, total_amount "
        f"FROM {RAW_SCHEMA}.flight_seat_reservations"
    )
    if min_id is not None and max_id is not None and min_id < max_id:
        df_fsr = read_pg(
            spark,
            fsr_sql,
            partition_column="flight_id",
            lower=min_id,
            upper=max_id,
            num_partitions=FSR_PARTITIONS,
        )
    else:
        df_fsr = read_pg(spark, fsr_sql)
    write_bronze(spark, add_metadata(df_fsr, run_id, cutoff_str), "flight_seat_reservations")

    # 4. Record the loaded cutoff only after all loads succeed
    logger.info("Updating watermarks to %s for run %s", cutoff_str, run_id)
    for table_name in BRONZE_TABLES:
        update_watermark(spark, table_name, cutoff_str, run_id)
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
