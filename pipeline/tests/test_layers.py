"""Comprehensive test suite for the Medallion pipeline layers.

Tests Silver & Gold transformations using local Spark and Iceberg with synthetic data.
Tests Publish driver functions using mongomock.
"""

import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import mongomock
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Jobs run under spark-submit as flat scripts (common.py shipped via --py-files),
# so they import `common` directly; mirror that layout here.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import ensure_namespaces  # noqa: E402
from gold import GOLD_TABLES, run_gold  # noqa: E402
from publish import (  # noqa: E402
    build_pipeline_runs_doc,
    clean_old_runs,
    create_indexes,
    record_pipeline_run,
)
from silver import run_silver  # noqa: E402

WAREHOUSE_DIR = "/tmp/ktdl-pipe-wh"
IVY_DIR = "/tmp/ktdl-ivy"

RAW_FLIGHT_SCHEMA = StructType([
    StructField("flight_id", IntegerType(), False),
    StructField("flight_no", StringType(), False),
    StructField("sched_dep", StringType(), False),
    StructField("sched_arr", StringType(), False),
    StructField("departure_airport", StringType(), False),
    StructField("arrival_airport", StringType(), False),
    StructField("status", StringType(), False),
    StructField("aircraft_code", StringType(), False),
    StructField("act_dep", StringType(), True),
    StructField("act_arr", StringType(), True),
])


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Create local SparkSession with Iceberg catalog pointing to scratch warehouse."""
    if os.path.exists(WAREHOUSE_DIR):
        shutil.rmtree(WAREHOUSE_DIR, ignore_errors=True)
    os.makedirs(WAREHOUSE_DIR, exist_ok=True)
    os.makedirs(IVY_DIR, exist_ok=True)
    spark_sess = (
        SparkSession.builder.appName("test-ktdl-pipeline")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0",
        )
        .config("spark.jars.ivy", IVY_DIR)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", f"file://{WAREHOUSE_DIR}")
        .config("spark.sql.defaultCatalog", "lake")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    ensure_namespaces(spark_sess)
    yield spark_sess
    spark_sess.stop()
    if os.path.exists(WAREHOUSE_DIR):
        shutil.rmtree(WAREHOUSE_DIR, ignore_errors=True)

@pytest.fixture(scope="session", autouse=True)
def setup_bronze_and_layers(spark: SparkSession) -> None:
    """Populate synthetic bronze data and execute silver and gold layers."""
    run_id_1 = "run_1"
    cutoff_str = "2017-08-15 18:00:00"
    ts_now_str = "2017-08-15 12:00:00"

    def with_meta(df: DataFrame, batch_id: str = run_id_1, ingest_str: str = ts_now_str) -> DataFrame:
        return (
            df.withColumn("_ingest_ts", F.to_timestamp(F.lit(ingest_str)))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_source_now", F.to_timestamp(F.lit(cutoff_str)))
        )

    # 1. aircrafts_data
    aircrafts = [
        ("773", '{"en": "Boeing 777-300", "ru": "Боинг 777-300"}', 11100),
        ("CN1", '{"en": "Cessna 208B Grand Caravan", "ru": "Сессна 208B"}', 1200),
        ("319", '{"en": "Airbus A319-100", "ru": "Аэробус A319-100"}', 6700),
    ]
    df_ac = with_meta(spark.createDataFrame(aircrafts, ["aircraft_code", "model", "range"]))
    df_ac.writeTo("lake.bronze.aircrafts_data").partitionedBy(F.days(F.col("_ingest_ts"))).create()

    # 2. airports_data
    airports = [
        ("VOZ", '{"en": "Voronezh Airport"}', '{"en": "Voronezh"}', "(39.2296,51.8143)", "Europe/Moscow"),
        ("LED", '{"en": "Pulkovo Airport"}', '{"en": "Saint Petersburg"}', "(30.2625,59.8003)", "Europe/Moscow"),
        ("YKS", '{"en": "Yakutsk Airport"}', '{"en": "Yakutsk"}', "(129.771,62.0933)", "Asia/Yakutsk"),
        ("VVO", '{"en": "Vladivostok Airport"}', '{"en": "Vladivostok"}', "(132.148,43.399)", "Asia/Vladivostok"),
    ]
    df_ap = with_meta(spark.createDataFrame(airports, ["airport_code", "airport_name", "city", "coordinates", "timezone"]))
    df_ap.writeTo("lake.bronze.airports_data").partitionedBy(F.days(F.col("_ingest_ts"))).create()

    # 3. seats
    seats = [
        ("773", "1A", "Business"),
        ("773", "2A", "Economy"),
        ("CN1", "1A", "Economy"),
        ("319", "1A", "Economy"),
        ("319", "1B", "Economy"),
    ]
    df_seats = with_meta(spark.createDataFrame(seats, ["aircraft_code", "seat_no", "fare_conditions"]))
    df_seats.writeTo("lake.bronze.seats").partitionedBy(F.days(F.col("_ingest_ts"))).create()

    # 4. bookings
    bookings = [
        ("B00001", "2017-08-14 10:00:00", 8000.0),
        ("B00002", "2017-08-14 11:00:00", 2000.0),
        ("B00003", "2017-08-14 12:00:00", 1000.0),
    ]
    df_b = spark.createDataFrame(bookings, ["book_ref", "book_date_str", "total_amount"])
    df_b = df_b.withColumn("book_date", F.to_timestamp(F.col("book_date_str"))).drop("book_date_str")
    df_b = with_meta(df_b)
    df_b.writeTo("lake.bronze.bookings").partitionedBy(F.days(F.col("_ingest_ts"))).create()

    # 5. tickets
    tickets = [
        ("T000000000001", "B00001", "1111 222222", "IVAN IVANOV", '{"email": "ivan@example.com"}'),
        ("T000000000002", "B00001", "3333 444444", "PETR PETROV", '{"email": "petr@example.com"}'),
        ("T000000000003", "B00002", "5555 666666", "ANNA SIDOROVA", '{"email": "anna@example.com"}'),
    ]
    df_t = with_meta(spark.createDataFrame(tickets, ["ticket_no", "book_ref", "passenger_id", "passenger_name", "contact_data"]))
    df_t.writeTo("lake.bronze.tickets").partitionedBy(F.days(F.col("_ingest_ts"))).create()

    # 6. flights:
    # Flight 1: YKS->VVO scheduled 2017-08-14 22:30:00 UTC (Monday UTC, but Tuesday in Asia/Yakutsk local time: +9h -> 07:30)
    #           Actual dep: 2017-08-14 22:50:00 (20 min delay > 15 min -> delayed). Status: Arrived. Aircraft: 773.
    # Flight 2: VOZ->LED scheduled 2017-08-14 10:00:00 UTC. Actual dep: 2017-08-14 10:05:00 (5 min delay <= 15 min -> not delayed). Status: Arrived. Aircraft: 773.
    # Flights 3..22: VOZ->LED to reach >= 20 Arrived flights on route VOZ->LED.
    # Flight 50: Scheduled flight to be modified in run_2.
    # Flight 60: Duplicate flight in run_1 for deduplication test.
    flight_rows = [
        (1, "PG0001", "2017-08-14 22:30:00", "2017-08-15 01:30:00", "YKS", "VVO", "Arrived", "773", "2017-08-14 22:50:00", "2017-08-15 01:55:00"),
        (2, "PG0002", "2017-08-14 10:00:00", "2017-08-14 12:00:00", "VOZ", "LED", "Arrived", "773", "2017-08-14 10:05:00", "2017-08-14 12:10:00"),
    ]
    for i in range(3, 23):
        flight_rows.append((
            i,
            f"PG00{i:02d}",
            "2017-08-14 10:00:00",
            "2017-08-14 12:00:00",
            "VOZ",
            "LED",
            "Arrived",
            "319",
            "2017-08-14 10:20:00" if i == 3 else "2017-08-14 10:00:00",
            "2017-08-14 12:25:00" if i == 3 else "2017-08-14 12:00:00",
        ))

    # Flight 50 initial run_1 status: Scheduled
    flight_rows.append((
        50, "PG0050", "2017-08-15 14:00:00", "2017-08-15 16:00:00",
        "VOZ", "LED", "Scheduled", "773", None, None
    ))

    # Flight 60 duplicate within run_1: earlier timestamp row
    flight_rows.append((
        60, "PG0060", "2017-08-15 08:00:00", "2017-08-15 10:00:00",
        "VOZ", "LED", "Scheduled", "CN1", None, None
    ))
    df_raw = spark.createDataFrame(flight_rows, schema=RAW_FLIGHT_SCHEMA)
    df_f = (
        df_raw.withColumn("scheduled_departure", F.to_timestamp(F.col("sched_dep")))
        .withColumn("scheduled_arrival", F.to_timestamp(F.col("sched_arr")))
        .withColumn("actual_departure", F.to_timestamp(F.col("act_dep")))
        .withColumn("actual_arrival", F.to_timestamp(F.col("act_arr")))
        .drop("sched_dep", "sched_arr", "act_dep", "act_arr")
    )
    df_f = with_meta(df_f, ingest_str="2017-08-15 11:00:00")

    # Add duplicate of flight 60 with LATER _ingest_ts (13:00:00) and status Departed
    dup_row = [(
        60, "PG0060", "2017-08-15 08:00:00", "2017-08-15 10:00:00",
        "VOZ", "LED", "Departed", "CN1", "2017-08-15 08:05:00", None,
    )]
    df_dup = (
        spark.createDataFrame(dup_row, schema=RAW_FLIGHT_SCHEMA)
        .withColumn("scheduled_departure", F.to_timestamp(F.col("sched_dep")))
        .withColumn("scheduled_arrival", F.to_timestamp(F.col("sched_arr")))
        .withColumn("actual_departure", F.to_timestamp(F.col("act_dep")))
        .withColumn("actual_arrival", F.to_timestamp(F.col("act_arr")))
        .drop("sched_dep", "sched_arr", "act_dep", "act_arr")
    )
    df_dup = with_meta(df_dup, ingest_str="2017-08-15 13:00:00")

    df_f_all = df_f.unionByName(df_dup)
    df_f_all.writeTo("lake.bronze.flights").partitionedBy(F.days(F.col("_ingest_ts"))).create()

    # 7. ticket_flights
    ticket_flights = [
        ("T000000000001", 1, "Business", 5000.0),
        ("T000000000002", 1, "Economy", 3000.0),
        ("T000000000003", 2, "Economy", 2000.0),
        # Bad ticket_flights row: amount < 0 to test quarantine
        ("T000000000001", 2, "Economy", -500.0),
    ]
    df_tf = with_meta(spark.createDataFrame(ticket_flights, ["ticket_no", "flight_id", "fare_conditions", "amount"]))
    df_tf.writeTo("lake.bronze.ticket_flights").partitionedBy(F.days(F.col("_ingest_ts"))).create()

    # 8. boarding_passes
    # Flight 1: 2 boarding passes on 773 (seats total 2) -> load factor 2/2 = 1.0
    # Flight 2: 1 boarding pass on 773 (seats total 2) -> load factor 1/2 = 0.5
    boarding_passes = [
        ("T000000000001", 1, 1, "1A"),
        ("T000000000002", 1, 2, "2A"),
        ("T000000000003", 2, 1, "1A"),
    ]
    df_bp = with_meta(spark.createDataFrame(boarding_passes, ["ticket_no", "flight_id", "boarding_no", "seat_no"]))
    df_bp.writeTo("lake.bronze.boarding_passes").partitionedBy(F.days(F.col("_ingest_ts"))).create()

    # Run Silver for run_1
    run_silver(spark, run_id_1)

    # Run Gold for run_1
    run_gold(spark, run_id_1)


def test_silver_dedup_within_batch(spark: SparkSession) -> None:
    """Assert (b): duplicates within a batch are deduped to the latest ingest timestamp."""
    rows = spark.sql("SELECT flight_id, status FROM lake.silver.flights_enriched WHERE flight_id = 60").collect()
    assert len(rows) == 1, f"Expected exactly 1 row for flight_id=60, got {len(rows)}"
    assert rows[0]["status"] == "Departed", f"Expected latest status 'Departed', got '{rows[0]['status']}'"


def test_silver_quarantine_and_rerun_idempotency(spark: SparkSession) -> None:
    """Assert (c): ticket_flights amount < 0 lands in quarantine, not silver, and rerun does not duplicate."""
    # Check not in silver
    silver_bad = spark.sql(
        "SELECT * FROM lake.silver.ticket_flights WHERE ticket_no = 'T000000000001' AND flight_id = 2"
    ).collect()
    assert len(silver_bad) == 0, "Quarantined ticket_flight row should not be in lake.silver.ticket_flights"

    # Check in quarantine
    q_rows = spark.sql(
        "SELECT * FROM lake.silver.quarantine WHERE source_table = 'ticket_flights' AND _batch_id = 'run_1'"
    ).collect()
    assert len(q_rows) == 1, f"Expected 1 quarantined row, got {len(q_rows)}"
    assert q_rows[0]["reason"] == "amount < 0"

    # Re-run silver for run_1 and verify quarantine rows count does not duplicate
    run_silver(spark, "run_1")
    q_rows_after = spark.sql(
        "SELECT * FROM lake.silver.quarantine WHERE source_table = 'ticket_flights' AND _batch_id = 'run_1'"
    ).collect()
    assert len(q_rows_after) == 1, f"Quarantine count after rerun should remain 1, got {len(q_rows_after)}"


def test_silver_merge_updates_status(spark: SparkSession) -> None:
    """Assert (a): silver MERGE: run 2 changes a flight from Scheduled to Arrived with actuals -> silver keeps 1 row."""
    # Check flight 50 is currently Scheduled
    f50_init = spark.sql("SELECT status, actual_departure FROM lake.silver.flights_enriched WHERE flight_id = 50").collect()
    assert len(f50_init) == 1
    assert f50_init[0]["status"] == "Scheduled"
    assert f50_init[0]["actual_departure"] is None

    # Ingest flight 50 in run_2 with status Arrived and actuals
    run_id_2 = "run_2"
    df_f2 = (
        spark.createDataFrame([(
            50, "PG0050", "2017-08-15 14:00:00", "2017-08-15 16:00:00",
            "VOZ", "LED", "Arrived", "773", "2017-08-15 14:10:00", "2017-08-15 16:15:00"
        )], schema=RAW_FLIGHT_SCHEMA)
        .withColumn("scheduled_departure", F.to_timestamp(F.col("sched_dep")))
        .withColumn("scheduled_arrival", F.to_timestamp(F.col("sched_arr")))
        .withColumn("actual_departure", F.to_timestamp(F.col("act_dep")))
        .withColumn("actual_arrival", F.to_timestamp(F.col("act_arr")))
        .drop("sched_dep", "sched_arr", "act_dep", "act_arr")
        .withColumn("_ingest_ts", F.to_timestamp(F.lit("2017-08-15 17:00:00")))
        .withColumn("_batch_id", F.lit(run_id_2))
        .withColumn("_source_now", F.to_timestamp(F.lit("2017-08-15 18:00:00")))
    )
    df_f2.writeTo("lake.bronze.flights").append()

    # Run silver for run_2
    run_silver(spark, run_id_2)

    # Verify silver keeps exactly 1 row with new status and actuals
    f50_after = spark.sql(
        "SELECT flight_id, status, actual_departure, actual_arrival "
        "FROM lake.silver.flights_enriched WHERE flight_id = 50"
    ).collect()
    assert len(f50_after) == 1, f"Expected 1 row for flight_id=50, got {len(f50_after)}"
    assert f50_after[0]["status"] == "Arrived"
    assert f50_after[0]["actual_departure"] is not None


def test_gold_delays_and_local_dow(spark: SparkSession) -> None:
    """Assert (d): gold delay_by_route / delay_heatmap: delayed means >15 min, Arrived only;

    dow is Monday=1 from local time (using flight whose UTC day differs from local day).
    """
    # Flight 1 was scheduled at 2017-08-14 22:30:00 UTC (Monday UTC),
    # but in Asia/Yakutsk (+9h) it is 2017-08-15 07:30:00 (Tuesday local time -> dow=2).
    # Actual departure was delayed by 20 min (>15 min).
    hm_rows = spark.sql("SELECT * FROM lake.gold.gold_delay_heatmap WHERE _id = '2|7'").collect()
    assert len(hm_rows) == 1, f"Expected 1 heatmap cell for dow=2, hour=7, got {len(hm_rows)}"
    hm = hm_rows[0]
    assert hm["dow"] == 2, f"Expected dow=2 (Tuesday from local time), got {hm['dow']}"
    assert hm["hour"] == 7
    assert hm["delayed"] >= 1
    assert hm["delay_rate"] > 0.0

    # Route delay: VOZ -> LED has >= 20 Arrived flights
    route_rows = spark.sql("SELECT * FROM lake.gold.gold_delay_by_route WHERE dep_airport = 'VOZ' AND arr_airport = 'LED'").collect()
    assert len(route_rows) == 1, f"Expected 1 route delay row for VOZ->LED, got {len(route_rows)}"
    rr = route_rows[0]
    assert rr["flights"] >= 20
    assert rr["delayed"] >= 1
    assert rr["avg_delay_min"] > 15.0


def test_gold_flight_occupancy_and_fleet(spark: SparkSession) -> None:
    """Assert (e): gold flight_occupancy load_factor = boarded/seats, and fleet avg."""
    # Flight 1 on 773: 2 boarded / 2 seats = 1.0
    # Flight 2 on 773: 1 boarded / 2 seats = 0.5
    occ_f1 = spark.sql("SELECT * FROM lake.gold.gold_flight_occupancy WHERE flight_id = 1").collect()[0]
    assert occ_f1["boarded"] == 2
    assert occ_f1["seats"] == 2
    assert occ_f1["load_factor"] == pytest.approx(1.0, 1e-4)

    occ_f2 = spark.sql("SELECT * FROM lake.gold.gold_flight_occupancy WHERE flight_id = 2").collect()[0]
    assert occ_f2["boarded"] == 1
    assert occ_f2["seats"] == 2
    assert occ_f2["load_factor"] == pytest.approx(0.5, 1e-4)

    # Fleet for 773: avg_load_factor = (1.0 + 0.5) / 2 = 0.75
    fleet_773 = spark.sql("SELECT * FROM lake.gold.gold_fleet WHERE aircraft_code = '773'").collect()[0]
    assert fleet_773["seats_total"] == 2
    assert fleet_773["seats_business"] == 1
    assert fleet_773["seats_economy"] == 1
    assert fleet_773["avg_load_factor"] == pytest.approx(0.75, 1e-4)


def test_gold_route_pareto_monotonic_and_ends_at_one(spark: SparkSession) -> None:
    """Assert (f): gold route_pareto cum_share is monotonic and ends at 1.0."""
    pareto_rows = spark.sql("SELECT rank, cum_share, revenue_share FROM lake.gold.gold_route_pareto ORDER BY rank").collect()
    assert len(pareto_rows) >= 2, f"Expected at least 2 routes, got {len(pareto_rows)}"

    prev_share = 0.0
    for r in pareto_rows:
        curr = r["cum_share"]
        assert curr >= prev_share, f"cum_share must be monotonic: {curr} < {prev_share}"
        prev_share = curr

    assert pareto_rows[-1]["cum_share"] == pytest.approx(1.0, 1e-4)


def test_gold_tables_exact_mongo_schema(spark: SparkSession) -> None:
    """Assert (g): every gold table has exactly the contract's Mongo field names including _id."""
    expected_schemas = {
        "gold_route_revenue": {
            "_id", "dep_airport", "arr_airport", "dep_city", "arr_city",
            "month", "fare_conditions", "revenue", "tickets",
        },
        "gold_route_pareto": {
            "_id", "dep_airport", "arr_airport", "dep_city", "arr_city",
            "revenue", "rank", "cum_share", "revenue_share",
        },
        "gold_flight_occupancy": {
            "_id", "flight_id", "flight_no", "dep_airport", "arr_airport",
            "aircraft_code", "model", "month", "scheduled_departure",
            "boarded", "seats", "load_factor",
        },
        "gold_fleet": {
            "_id", "aircraft_code", "model", "range", "seats_total",
            "seats_economy", "seats_comfort", "seats_business", "flights",
            "flight_hours", "avg_duration_min", "avg_load_factor",
        },
        "gold_delay_heatmap": {
            "_id", "dow", "hour", "flights", "delayed", "delay_rate",
        },
        "gold_delay_by_aircraft": {
            "_id", "aircraft_code", "model", "flights", "delayed",
            "delay_rate", "avg_delay_min",
        },
        "gold_delay_by_route": {
            "_id", "dep_airport", "arr_airport", "dep_city", "arr_city",
            "flights", "delayed", "delay_rate", "avg_delay_min",
        },
        "dim_airports": {
            "_id", "airport_code", "airport_name", "city", "lon", "lat",
            "timezone", "departures",
        },
        "dim_routes": {
            "_id", "dep_airport", "arr_airport", "dep_lon", "dep_lat",
            "arr_lon", "arr_lat", "flights",
        },
    }

    for table_name, expected_fields in expected_schemas.items():
        actual_fields = set(spark.table(f"lake.gold.{table_name}").columns)
        assert actual_fields == expected_fields, (
            f"Schema mismatch in lake.gold.{table_name}:\n"
            f"Missing: {expected_fields - actual_fields}\n"
            f"Unexpected: {actual_fields - expected_fields}"
        )


def test_publish_cleanup_and_indexes() -> None:
    """Test publish cleanup and index creation functions using mongomock."""
    client = mongomock.MongoClient()
    db = client["airlines"]

    # Test clean_old_runs
    db["gold_route_revenue"].insert_many([
        {"_id": "VOZ|LED|2017-08|Economy", "_run_id": "run_old", "revenue": 100.0},
        {"_id": "VOZ|LED|2017-08|Business", "_run_id": "run_current", "revenue": 200.0},
    ])
    clean_counts = clean_old_runs(db, ["gold_route_revenue"], "run_current")
    assert clean_counts["gold_route_revenue"] == 1
    remaining = list(db["gold_route_revenue"].find())
    assert len(remaining) == 1
    assert remaining[0]["_run_id"] == "run_current"

    # Test create_indexes
    create_indexes(db, ["gold_route_revenue", "gold_fleet"])
    rev_indexes = db["gold_route_revenue"].index_information()
    assert "month_1" in rev_indexes
    assert "dep_airport_1" in rev_indexes

    fleet_indexes = db["gold_fleet"].index_information()
    assert "aircraft_code_1" in fleet_indexes


def test_publish_pipeline_runs_doc_builder() -> None:
    """Test pipeline_runs doc builder and recording into mongomock."""
    client = mongomock.MongoClient()
    db = client["airlines"]

    started = datetime(2017, 8, 15, 12, 0, 0, tzinfo=timezone.utc)
    finished = datetime(2017, 8, 15, 12, 5, 0, tzinfo=timezone.utc)
    cutoff = datetime(2017, 8, 15, 18, 0, 0, tzinfo=timezone.utc)

    counts = {
        "bronze": {"flights": 10},
        "silver": {"flights_enriched": 10},
        "gold": {"gold_fleet": 3},
    }

    doc = build_pipeline_runs_doc(
        run_id="run_100",
        cutoff=cutoff,
        started_at=started,
        finished_at=finished,
        status="success",
        counts=counts,
        quarantine=0,
    )

    assert doc["_id"] == "run_100"
    assert doc["run_id"] == "run_100"
    assert doc["status"] == "success"
    assert doc["counts"] == counts
    assert doc["quarantine"] == 0

    record_pipeline_run(db, doc)
    saved = db["pipeline_runs"].find_one({"_id": "run_100"})
    assert saved is not None
    assert saved["run_id"] == "run_100"
    assert saved["status"] == "success"
