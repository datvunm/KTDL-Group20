"""Silver transformation layer for Airlines Lakehouse.

Runs SQL transformations in order:
00_quarantine -> 01_airports -> 02_aircrafts -> 03_seats -> 04_bookings
-> 05_tickets -> 06_flights_enriched -> 07_ticket_flights -> 08_boarding_passes.

Applies deduplication, validation, quarantine routing, and MERGE INTO logic.
"""

import logging
from pathlib import Path

from pyspark.sql import SparkSession

from common import ensure_namespaces, get_spark, parse_args, run_sql_file

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run_silver(spark: SparkSession, run_id: str, sql_dir: Path = None) -> None:
    """Execute silver layer SQL scripts sequentially."""
    ensure_namespaces(spark)

    if sql_dir is None:
        sql_dir = Path(__file__).resolve().parent / "sql" / "silver"

    sql_files = sorted(sql_dir.glob("*.sql"))
    if not sql_files:
        raise FileNotFoundError(f"No SQL files found in {sql_dir}")

    logger.info("Executing %d silver SQL scripts for run_id=%s...", len(sql_files), run_id)
    for sql_file in sql_files:
        logger.info("Running silver script: %s", sql_file.name)
        run_sql_file(spark, str(sql_file), run_id=run_id)

    logger.info("Silver transformation for run_id=%s completed successfully", run_id)


def main() -> None:
    args = parse_args("Silver Transformation Job")
    spark = get_spark("airlines-silver")
    try:
        run_silver(spark, args.run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
