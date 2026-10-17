# Airlines Lakehouse Pipeline

Batch Medallion data pipeline built with Apache Spark 3.5, Apache Iceberg, and MongoDB.

## Architecture

```
PostgreSQL (demo DB, schema raw = simulated state as of raw.now())
  [  Raw   ]  raw.flight_seat_reservations, raw.airport_sites, raw.aircraft_seat_layouts
             (committed raw dump + source/simulate.sh, see source/README.md)
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
├── stream_main.py           # Runs stream_bookings.py + stream_flights.py as one Spark app
├── stream_bookings.py       # Kafka booking.created -> bronze + silver (bookings/tickets/ticket_flights)
├── stream_flights.py        # Kafka flight.departed/arrived -> bronze + silver flights_enriched
├── sql/
│   ├── silver/              # Numbered Silver DDL & MERGE scripts (00 to 08), raw -> entities
│   ├── gold/                # Gold mart CREATE OR REPLACE TABLE AS SELECT scripts
│   ├── stream_bookings/     # Same pattern as sql/silver/, run per Kafka micro-batch
│   └── stream_flights/      # Same pattern as sql/silver/, run per Kafka micro-batch
├── tests/
│   ├── __init__.py
│   └── test_layers.py       # Comprehensive pytest suite on synthetic data
└── README.md
```

## Streaming

`stream_main.py` runs two Structured Streaming queries in one Spark app (compose service `spark-stream`): one consumes `booking.created`, the other `flight.departed`/`flight.arrived`. Producers live in `kafka/producer/`. See `docs/PIPELINE.md` for how it fits with the batch path, and `start-streaming.sh` to run it.

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
