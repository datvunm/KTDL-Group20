# Airlines Lakehouse Pipeline

Batch Medallion data pipeline built with Apache Spark 3.5, Apache Iceberg, and MongoDB.

## Architecture

```
PostgreSQL (demo DB, schema bookings = simulated state)
       │
       ▼  (Airflow prepare_raw_source: sql/bronze/airline_data_source.sql)
  [  Raw   ]  raw.flight_seat_reservations, raw.airport_sites, raw.aircraft_seat_layouts
       │
       ▼  (JDBC full snapshot, cast complex types, partitioned read)
  [ Bronze ]  lake.bronze.* (3 append-only Iceberg tables, partitioned by days(_ingest_ts), _batch_id)
       │
       ▼  (Split raw rows into entities, conflict/quality quarantine, enrichment, MERGE INTO)
  [ Silver ]  lake.silver.* (8 Iceberg dimension & fact tables + quarantine)
       │
       ▼  (Aggregation, analytics metrics, Gonor.me / Postgres docs formulas)
  [  Gold  ]  lake.gold.* (Marts & dims) + snapshot expiration
       │
       ▼  (Mongo Spark connector + PyMongo driver maintenance)
  [ Publish]  MongoDB `airlines` database + pipeline_runs metadata
```

## Structure

```
pipeline/
├── common.py                # SparkSession init, JDBC helpers, SQL runner, CLI parser
├── bronze.py                # Bronze ingestion of the 3 raw tables from PG to Iceberg
├── silver.py                # Silver transforms with quarantine routing & MERGE
├── gold.py                  # Gold marts aggregation & snapshot expiration
├── publish.py               # Gold publishing to Mongo & driver metadata/indexes
├── sql/
│   ├── bronze/              # airline_data_source.sql: raw source DDL + validation (run by Airflow)
│   ├── silver/              # Numbered Silver DDL & MERGE scripts (00 to 08), raw -> entities
│   └── gold/                # Gold mart CREATE OR REPLACE TABLE AS SELECT scripts
├── tests/
│   ├── __init__.py
│   └── test_layers.py       # Comprehensive pytest suite on synthetic data
└── README.md
```

## Invocations

### Via `spark-submit` (Airflow / Docker):

```bash
# Bronze
spark-submit --master spark://spark:7077 \
  --py-files /opt/pipeline/common.py /opt/pipeline/bronze.py --run-id 20261004_120000

# Silver
spark-submit --master spark://spark:7077 \
  --py-files /opt/pipeline/common.py /opt/pipeline/silver.py --run-id 20261004_120000

# Gold
spark-submit --master spark://spark:7077 \
  --py-files /opt/pipeline/common.py /opt/pipeline/gold.py --run-id 20261004_120000

# Publish
spark-submit --master spark://spark:7077 \
  --py-files /opt/pipeline/common.py /opt/pipeline/publish.py --run-id 20261004_120000
```

### Standalone Python execution:

When run standalone, `common.py` dynamically configures the Iceberg catalog and settings.

```bash
python pipeline/bronze.py --run-id <run_id>
python pipeline/silver.py --run-id <run_id>
python pipeline/gold.py --run-id <run_id>
python pipeline/publish.py --run-id <run_id>
```

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `PG_URL` | `jdbc:postgresql://db:5432/demo` | PostgreSQL JDBC connection URL |
| `PG_USER` | `postgres` | PostgreSQL username |
| `PG_PASSWORD` | `123456` | PostgreSQL password |
| `LAKE_WAREHOUSE` | `hdfs://namenode:9000/warehouse` | Iceberg catalog warehouse URI |
| `MONGO_URI` | `mongodb://root:123456@mongo:27017/?authSource=admin` | MongoDB connection URI |
| `MONGO_DB` | `airlines` | MongoDB target database |
| `SPARK_SHUFFLE_PARTITIONS` | `8` | Spark SQL shuffle partitions |

## Testing

Run local tests with synthetic data and mongomock:

```bash
pytest pipeline/tests/test_layers.py -v
```
