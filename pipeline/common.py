"""Common utilities for the Airlines Lakehouse Spark pipeline.

Provides SparkSession initialization, JDBC read helpers, SQL runner with
parameter substitution, namespace management, and CLI argument parsing.
"""

import argparse
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional

# Ensure Java 17/21 compatibility with Spark reflection
_REQUIRED_JAVA_OPENS = (
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED"
)
if "JAVA_TOOL_OPTIONS" not in os.environ:
    os.environ["JAVA_TOOL_OPTIONS"] = _REQUIRED_JAVA_OPENS
elif "--add-opens=java.base/java.nio=ALL-UNNAMED" not in os.environ["JAVA_TOOL_OPTIONS"]:
    os.environ["JAVA_TOOL_OPTIONS"] = f"{os.environ['JAVA_TOOL_OPTIONS']} {_REQUIRED_JAVA_OPENS}"

from pyspark.sql import DataFrame, SparkSession


def parse_args(description: str = "Medallion Pipeline Job") -> argparse.Namespace:
    """Parse CLI arguments for pipeline jobs."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--run-id",
        dest="run_id",
        required=True,
        help="Batch run identifier (e.g. 20261004_120000)",
    )
    return parser.parse_args()


def get_spark(app_name: str = "airlines-pipeline") -> SparkSession:
    """Create or retrieve a SparkSession with Iceberg and catalog configurations.

    When catalog configs are absent (e.g. standalone script execution),
    configures the 'lake' Hadoop catalog targeting LAKE_WAREHOUSE.
    """
    warehouse = os.environ.get("LAKE_WAREHOUSE", "hdfs://namenode:9000/warehouse")
    shuffle_partitions = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
    )

    # Ivy and packages if running standalone without spark-submit
    if "SPARK_JARS_PACKAGES" in os.environ:
        builder = builder.config("spark.jars.packages", os.environ["SPARK_JARS_PACKAGES"])
    if "SPARK_JARS_IVY" in os.environ:
        builder = builder.config("spark.jars.ivy", os.environ["SPARK_JARS_IVY"])

    return builder.getOrCreate()


def read_pg(
    spark: SparkSession,
    query: str,
    partition_column: Optional[str] = None,
    lower: Optional[Any] = None,
    upper: Optional[Any] = None,
    num_partitions: Optional[int] = None,
) -> DataFrame:
    """Read data from PostgreSQL using JDBC with query wrapping and fetchsize 10000."""
    pg_url = os.environ.get("PG_URL", "jdbc:postgresql://db:5432/demo")
    pg_user = os.environ.get("PG_USER", "postgres")
    pg_password = os.environ.get("PG_PASSWORD", "123456")

    reader = (
        spark.read.format("jdbc")
        .option("url", pg_url)
        .option("dbtable", f"({query}) q")
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
    )

    if partition_column and lower is not None and upper is not None and num_partitions:
        reader = (
            reader.option("partitionColumn", partition_column)
            .option("lowerBound", str(lower))
            .option("upperBound", str(upper))
            .option("numPartitions", str(num_partitions))
        )

    return reader.load()


def run_sql_file(spark: SparkSession, path: str, **params: Any) -> Optional[DataFrame]:
    """Execute a SQL file with ${param} variable substitution.

    Statements are split on lines ending with ';' (ignoring comments).
    Returns the DataFrame resulting from the last executed statement.
    """
    raw_content = Path(path).read_text(encoding="utf-8")

    # Substitute parameters ${name}
    content = raw_content
    for k, v in params.items():
        content = content.replace(f"${{{k}}}", str(v))

    statements = []
    current_lines = []

    for line in content.splitlines():
        stripped = line.strip()
        # Skip top-level comment lines if statement buffer is empty
        if not current_lines and (stripped.startswith("--") or stripped.startswith("/*")):
            continue

        current_lines.append(line)
        if stripped.endswith(";") and not stripped.startswith("--"):
            stmt = "\n".join(current_lines).strip()
            if stmt.endswith(";"):
                stmt = stmt[:-1].strip()
            if stmt:
                statements.append(stmt)
            current_lines = []

    if current_lines:
        stmt = "\n".join(current_lines).strip()
        if stmt.endswith(";"):
            stmt = stmt[:-1].strip()
        if stmt:
            statements.append(stmt)

    last_df: Optional[DataFrame] = None
    for stmt in statements:
        clean_stmt = stmt.strip()
        if clean_stmt:
            last_df = spark.sql(clean_stmt)

    return last_df


def ensure_namespaces(spark: SparkSession) -> None:
    """Create lake namespaces if they do not already exist."""
    for ns in ["lake.bronze", "lake.silver", "lake.gold", "lake.meta"]:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
