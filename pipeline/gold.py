"""Gold layer aggregation and marts generation for Airlines Lakehouse.

Creates analytical marts and dimensional tables in lake.gold:
- gold_route_revenue
- gold_route_pareto
- gold_flight_occupancy
- gold_fleet
- gold_delay_heatmap
- gold_delay_by_aircraft
- gold_delay_by_route
- dim_airports
- dim_routes

Follows metric definitions from https://analytics-flights.gonor.me and
official Postgres demo DB documentation.
Performs snapshot expiration after gold build to conserve storage.
"""

import logging
from pathlib import Path
from typing import List, Optional

from pyspark.sql import SparkSession

from common import ensure_namespaces, get_spark, parse_args, run_sql_file

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

GOLD_SCRIPTS_ORDER = [
    "gold_route_revenue.sql",
    "gold_route_pareto.sql",
    "gold_flight_occupancy.sql",
    "gold_fleet.sql",
    "gold_delay_heatmap.sql",
    "gold_delay_by_aircraft.sql",
    "gold_delay_by_route.sql",
    "dim_airports.sql",
    "dim_routes.sql",
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
    "quarantine",
]

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


def expire_snapshots(spark: SparkSession, tables: List[str], retain_last: int = 3) -> None:
    """Run Iceberg expire_snapshots procedure on specified tables."""
    for table in tables:
        try:
            logger.info("Expiring snapshots for %s (retain_last=%d)...", table, retain_last)
            spark.sql(f"CALL lake.system.expire_snapshots(table => '{table}', retain_last => {retain_last})")
        except Exception as e:
            logger.warning("Could not expire snapshots for %s: %s", table, e)


def run_gold(spark: SparkSession, run_id: str, sql_dir: Optional[Path] = None) -> None:
    """Execute gold marts generation and snapshot maintenance."""
    ensure_namespaces(spark)

    if sql_dir is None:
        sql_dir = Path(__file__).resolve().parent / "sql" / "gold"

    logger.info("Executing %d gold scripts in sequence for run_id=%s...", len(GOLD_SCRIPTS_ORDER), run_id)
    for script_name in GOLD_SCRIPTS_ORDER:
        script_path = sql_dir / script_name
        if not script_path.exists():
            raise FileNotFoundError(f"Required gold script not found: {script_path}")
        logger.info("Running gold script: %s", script_name)
        run_sql_file(spark, str(script_path), run_id=run_id)

    # Snapshot hygiene: retain last 3 snapshots to bound storage growth
    all_tables = [f"lake.silver.{t}" for t in SILVER_TABLES] + [f"lake.gold.{t}" for t in GOLD_TABLES]
    expire_snapshots(spark, all_tables, retain_last=3)

    logger.info("Gold marts generation for run_id=%s completed successfully", run_id)


def main() -> None:
    args = parse_args("Gold Marts Generation Job")
    spark = get_spark("airlines-gold")
    try:
        run_gold(spark, args.run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
