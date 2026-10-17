# Data Pipeline: Flow & Transformations

How data moves from the simulator through Bronze/Silver/Gold to MongoDB, plus the streaming speed layer. See [ARCHITECTURE.md](ARCHITECTURE.md) for the container topology, [DASHBOARD.md](DASHBOARD.md) for the UI that consumes this.

## Table of Contents

- [End-to-End Data Flow](#end-to-end-data-flow)
  - [Operational Source & Simulation Architecture](#operational-source--simulation-architecture)
  - [Airflow Medallion DAG Execution](#airflow-medallion-dag-execution)
  - [Streaming Speed Layer (Kafka + Spark Structured Streaming)](#streaming-speed-layer-kafka--spark-structured-streaming)
  - [Serving Layer & FastAPI Ingestion](#serving-layer--fastapi-ingestion)
- [Core Logic & Transformations](#core-logic--transformations)
  - [Deterministic Temporal Simulator](#deterministic-temporal-simulator)
  - [Raw Source Tables](#raw-source-tables)
  - [Bronze Ingestion & Watermarking](#bronze-ingestion--watermarking)
  - [Silver Cleansing, Quarantine & Enrichment](#silver-cleansing-quarantine--enrichment)
  - [Gold Analytical Marts & Industry Formulations](#gold-analytical-marts--industry-formulations)
  - [Publish Layer, PyMongo Maintenance & Lifecycle Hygiene](#publish-layer-pymongo-maintenance--lifecycle-hygiene)

---

## End-to-End Data Flow

```
[Committed Raw Dump: source/data/raw (3 denormalized tables, gzip parts)]
       │
       ▼  (source/load_dump.sh)
[PostgreSQL: archive schema] (Full history)
       │
       ▼  (source/simulate.sh <cutoff>)
[PostgreSQL: raw schema] (State of the 3 raw tables as of raw.now())
       │
       ▼ (1. check_source: PythonOperator)
[Airflow DAG: airlines_medallion]
       │
       ▼ (2. spark-submit bronze.py)
[HDFS: lake.bronze.* (3 raw snapshot Iceberg tables)]
       │
       ▼ (3. spark-submit silver.py)
[HDFS: lake.silver.* (MERGE INTO)] ──► [HDFS: lake.silver.quarantine] (Conflicting / malformed rows)
       │
       ▼ (4. spark-submit gold.py)
[HDFS: lake.gold.* (Analytical Marts & Dims)] ──► [Iceberg snapshot expiration]
       │
       ▼ (5. spark-submit publish.py)
[MongoDB: airlines database (replace upsert & index builds)]
       │
       ▼
[FastAPI REST API & Interactive UI Dashboard (host :8005)]
```

### Operational Source & Simulation Architecture

1. **Raw dump (one-time conversion)**: the PostgresPro Medium English Airlines dump (`demo-medium-en-20170815.sql`, 8 normalized tables) was converted once by `source/export_raw_dump.sh` into 3 denormalized raw tables via [`source/build_raw.sql`](../source/build_raw.sql). A row-by-row recovery test confirmed the conversion is lossless (0 lost / 0 extra rows across all 8 original tables plus the derived route timetable). Committed to git as a split gzip stream under `source/data/raw/` (below GitHub's 100 MB limit), with `SHA256SUMS`.
2. **Initial baseline load**: `source/load_dump.sh` verifies the checksums, recreates database `demo`, and streams the parts into schema `archive` (state as of `2017-08-15 18:00:00+03`). It also runs `source/schema.sql`, which creates the same 3 tables in schema `raw`, an index on `raw.flight_seat_reservations(flight_id)`, and `raw.sim_state`.
3. **Dynamic clock function**:
   ```sql
   CREATE OR REPLACE FUNCTION raw.now()
   RETURNS timestamptz LANGUAGE sql STABLE AS $$
       SELECT cutoff FROM raw.sim_state LIMIT 1;
   $$;
   ```
   Marked `STABLE`, not `IMMUTABLE`, so the planner never folds it to a constant across queries. `ALTER DATABASE demo SET search_path = raw, public;` makes unqualified queries resolve against the live simulated state.

### Airflow Medallion DAG Execution

DAG `airlines_medallion` has no cron schedule (`schedule=None`) - triggered on demand.

1. **`check_source` (`PythonOperator`)**: connects via `psycopg2`, runs `SELECT raw.now();`, checks `raw.flight_seat_reservations` has booked rows (`ticket_no IS NOT NULL`), and pushes the cutoff to XCom.
2. **`bronze` (`SparkSubmitOperator`)**: submits `/opt/pipeline/bronze.py --run-id <id>`. Reads the 3 raw tables via JDBC, appends full snapshots to Iceberg.
3. **`silver` (`SparkSubmitOperator`)**: submits `/opt/pipeline/silver.py`. Reads the current Bronze batch (`WHERE _batch_id = '${run_id}'`), splits it into the 8 entities, validates, routes invalid rows to `lake.silver.quarantine`, merges the rest.
4. **`gold` (`SparkSubmitOperator`)**: submits `/opt/pipeline/gold.py`. Aggregates Silver into marts/dims, then runs `CALL lake.system.expire_snapshots(...)`.
5. **`publish` (`SparkSubmitOperator`)**: submits `/opt/pipeline/publish.py`. Syncs Gold marts to MongoDB (`replace` upsert), records row-count lineage into `pipeline_runs`, and builds compound indexes via PyMongo.
6. **`on_failure_callback`**: on any operator failure, writes a `pipeline_runs` document with `status: "failed"` so the dashboard reflects outages too.

### Streaming Speed Layer (Kafka + Spark Structured Streaming)

A second, always-on path replays real historical events from `archive.flight_seat_reservations` onto Kafka and writes into the same Silver tables the batch pipeline builds. It runs independently of the cutoff simulator - sourced from `archive` (full history), not `raw` (the cutoff-gated snapshot).

- **Why replay events, not CDC**: `simulate.sql` truncates and reinserts `raw.*` on every cutoff, which Postgres CDC would see as "delete everything, insert everything" each time. Two producers instead read `archive` once and replay its real timestamps (`book_date`, `actual_departure`, `actual_arrival`) in order, scaled by a speed factor. A cancelled flight has no recorded cancellation time in `archive`, so it's never replayed - cancellation stays a batch-only fact.
- **`booking.created`** (`kafka/producer/booking_producer.py`): one message per booking; each segment carries its own flight schedule (airports, aircraft, times), not just a flight id.
- **`flight.departed` / `flight.arrived`** (`kafka/producer/flight_producer.py`): one event per real `actual_departure` / `actual_arrival`.
- **`pipeline/stream_main.py`**: runs both consumers as one Spark app (compose service `spark-stream`), sharing one core (`spark.cores.max=1`, `spark.scheduler.mode=FAIR`) - a streaming query holds its cores until stopped, and the worker only has 2.
  - `stream_bookings.py` + `pipeline/sql/stream_bookings/*.sql`: appends to `lake.bronze.booking_events`, upserts `bookings`, `tickets`, `ticket_flights`, and `flights_enriched` (insert-only - creates a `Scheduled` placeholder if the batch hasn't loaded that flight yet, never overwrites a status the batch already set).
  - `stream_flights.py` + `pipeline/sql/stream_flights/*.sql`: appends to `lake.bronze.flight_events`, upserts `flights_enriched` with real status/actuals, same formulas as `06_flights_enriched.sql`. This one may overwrite - it's real recorded data.
  - Both delete-then-append their own `_batch_id` in bronze before writing, so a retried micro-batch doesn't duplicate rows.
- **Live dashboard panel**: `stream_bookings.py` also writes a per-minute booking count/revenue to Mongo's `live_bookings`. `GET /api/live/bookings` serves the last 20 minutes; the dashboard's Pipeline tab polls it every 5s.
- **Operational notes**: Kafka is a single-broker KRaft cluster; topics must exist before the consumer subscribes. Producer env vars: `SPEED_FACTOR`, `MAX_GAP_SECONDS`, `LIMIT_BOOKINGS` / `LIMIT_FLIGHTS`.

### Serving Layer & FastAPI Ingestion

1. **Decoupled serving**: FastAPI never queries Spark/HDFS at request time - all reads hit indexed MongoDB collections (`gold_route_revenue`, `gold_route_pareto`, `gold_fleet`, `gold_delay_heatmap`, `gold_delay_by_aircraft`, `gold_delay_by_route`, `dim_airports`, `dim_routes`, `pipeline_runs`).
2. **Client-side rendering**: `dashboard.html` fetches pre-computed marts through `/api/marts/*` and `/api/summary`, rendering charts without server-side templating.

---

## Core Logic & Transformations

### Deterministic Temporal Simulator

`source/simulate.sql`, run via `source/simulate.sh <cutoff>`, as one transaction (`SET LOCAL work_mem = '256MB'`). It reads `archive.*` (full history) and writes `raw.*` (state as of `:cutoff`), applying the same rules the original 8-table simulator applied to `bookings`, `tickets`, `flights`, `ticket_flights` and `boarding_passes`.

1. **Truncation**: the 3 `raw` tables are truncated.
2. **Static reference replication**: `airport_sites` and `aircraft_seat_layouts` are fully replicated from `archive` on every run (no cutoff gating).
3. **Booking visibility**: a booking (with its ticket/flight columns) is visible only if `book_date <= :cutoff`. Otherwise those columns are NULL (empty seat) or the row disappears.
4. **Flight release horizon (31 days)**: a flight is released if `scheduled_departure <= :cutoff + interval '31 days'`, OR it's already booked by a released booking (keeps advance bookings > 31 days out referentially intact). Route timetable columns (`days_of_week`, `duration`) are recomputed over released flights.
5. **Cutoff-aware flight state**:
   - `Cancelled`: stays `Cancelled`, actuals forced NULL.
   - `Arrived`: `actual_departure <= :cutoff` AND `actual_arrival <= :cutoff`.
   - `Departed`: `actual_departure <= :cutoff`, arrival still pending.
   - `Delayed` / `On Time`: not yet departed, but check-in is open (`scheduled_departure - 24h <= :cutoff`) - `Delayed` if `archive.status = 'Delayed'` OR `actual_departure > scheduled_departure`, else `On Time`.
   - `Scheduled`: check-in not yet open. Both actuals NULL.
6. **Boarding pass / 24h check-in rule**: a seat assignment (`seat_no` + `boarding_no`) only appears once the flight isn't `Cancelled` and check-in has opened:
   ```sql
   checkin_open = a.status <> 'Cancelled'
                  AND a.scheduled_departure - interval '24 hours' <= CAST(:cutoff AS timestamptz)
   ```
   Before that, the archive row splits into an empty-seat row and a seatless booking row.
7. **Acceptance invariants**: at the final archive timestamp (`2017-08-15 18:00:00+03`), `raw` matches `archive` exactly (`EXCEPT ALL` both directions = 0 rows for all 3 tables). Verified row-for-row against the original 8-table pipeline at 2017-05-01, 2017-05-20 12:00, 2017-06-15, 2017-07-15, and the final cutoff.

---

### Raw Source Tables

Three denormalized raw tables, identical in shape between `archive` (full history) and `raw` (simulated state), derived once from the 8 normalized demo tables by [`source/build_raw.sql`](../source/build_raw.sql):

| Raw table | Grain | Source tables folded in |
|---|---|---|
| `flight_seat_reservations` | One row per physical seat per flight (ticket columns NULL when empty), plus one row per booked segment without a boarding pass (seat columns NULL) | `flights`, derived route timetable (`days_of_week`, `duration`), `boarding_passes`, `ticket_flights`, `tickets`, `bookings` |
| `airport_sites` | One row per airport | `airports_data` |
| `aircraft_seat_layouts` | One row per seat per aircraft model | `aircrafts_data`, `seats` |

At the final cutoff `flight_seat_reservations` holds **5,513,920** rows (2,360,335 booked + 3,153,585 empty seats).

### Bronze Ingestion & Watermarking

`pipeline/bronze.py` copies the three raw tables as-is from PostgreSQL into append-only Iceberg tables: `lake.bronze.flight_seat_reservations`, `lake.bronze.airport_sites`, `lake.bronze.aircraft_seat_layouts`.

1. **Complex PostgreSQL types**: `jsonb`, `point`, `integer[]`, `interval` columns are cast to `::text` in the extraction query - raw values stay unparsed.
2. **Metadata columns**: `_ingest_ts` (UTC load time), `_batch_id` (Airflow run id), `_source_now` (cutoff from `raw.now()`).
3. **Partitioning**: `partitionedBy(days(_ingest_ts), _batch_id)` - the `_batch_id` partition lets Silver prune to the current batch.
4. **Full snapshots**: every run takes a full snapshot (the raw grain has no change-tracking column). `flight_seat_reservations` reads via an 8-way JDBC partitioned query on `flight_id`.
5. **Watermarks (`lake.meta.watermarks`)**: after all three loads succeed, the loaded cutoff is recorded per table via `MERGE INTO`.

---

### Silver Cleansing, Quarantine & Enrichment

`pipeline/silver.py` runs SQL scripts `00` through `08`: type standardization, dedup, validation, quarantine routing, PII hashing, geospatial/temporal enrichment.

1. **Raw → entity extraction**: each script builds a temp view with `SELECT DISTINCT <entity columns>` over the relevant raw table, collapsing the denormalized grain (a booking repeats on every seat/segment row; a flight repeats on every seat row):

   | Silver table | Raw source and filter |
   |---|---|
   | `airports` | `airport_sites` |
   | `aircrafts` | `aircraft_seat_layouts` (distinct code, model, range) |
   | `seats` | `aircraft_seat_layouts WHERE seat_no IS NOT NULL` |
   | `bookings` | `flight_seat_reservations WHERE book_ref IS NOT NULL` |
   | `tickets` | `flight_seat_reservations WHERE ticket_no IS NOT NULL` |
   | `flights_enriched` | `flight_seat_reservations` (every row carries its flight) |
   | `ticket_flights` | `flight_seat_reservations WHERE ticket_no IS NOT NULL` |
   | `boarding_passes` | `flight_seat_reservations WHERE ticket_no IS NOT NULL AND seat_no IS NOT NULL` |

2. **Quarantine routing**: each view tags rows with a `reject_reason`; tagged rows go to `lake.silver.quarantine` (`00_quarantine.sql` creates it and clears the current batch for idempotent reruns), only untagged rows merge:
   ```sql
   CREATE TABLE IF NOT EXISTS lake.silver.quarantine (
       source_table string,
       reason string,
       payload string,
       _batch_id string,
       _ingest_ts timestamp
   ) USING iceberg;
   ```
   - **Conflicting attributes** (all entities): a key with more than one distinct attribute set in the batch (e.g. one `book_ref` with two `book_date`s), via `count(*) OVER (PARTITION BY <key>) > 1`. Every version is quarantined.
   - **Flights**: `actual_arrival <= actual_departure`, status outside the six valid values, or airport/aircraft codes missing from Silver dimensions.
   - **Ticket flights**: `amount < 0`, invalid fare class, or `flight_id` / `ticket_no` not in Silver.
   - **Boarding passes**: missing `boarding_no`, or seat not on the flight's aircraft.
   - Reference tables: missing keys, invalid seat fare class, conflicting attributes.
3. **Primary-key `MERGE INTO`**: reconciles updates (e.g. a flight moving Scheduled → Arrived between cutoffs) and inserts idempotently:
   ```sql
   MERGE INTO lake.silver.airports AS target
   USING (SELECT ... FROM airports_batch WHERE reject_reason IS NULL) AS source
   ON target.airport_code = source.airport_code
   WHEN MATCHED THEN UPDATE SET *
   WHEN NOT MATCHED THEN INSERT *;
   ```
4. **PII hashing**: `passenger_id` is hashed (SHA-256) before landing in Silver (`05_tickets.sql`):
   ```sql
   sha2(passenger_id, 256) AS passenger_key
   ```
   Plaintext passenger names and contact info stay in Bronze only; quarantine payloads hash `passenger_id` too.
5. **Geospatial/temporal enrichment** (`06_flights_enriched.sql`):
   - Haversine distance between origin/destination, in Spark SQL directly:
     ```sql
     round(2 * 6371 * asin(sqrt(
       pow(sin(radians(arr.lat - dep.lat) / 2), 2) +
       cos(radians(dep.lat)) * cos(radians(arr.lat)) *
       pow(sin(radians(arr.lon - dep.lon) / 2), 2)
     )), 2) AS distance_km
     ```
   - Local departure time via the origin airport's IANA timezone:
     ```sql
     from_utc_timestamp(f.scheduled_departure, coalesce(dep.timezone, 'UTC')) AS scheduled_departure_local
     ```
   - Delay and duration:
     ```sql
     dep_delay_min = (unix_timestamp(actual_departure) - unix_timestamp(scheduled_departure)) / 60.0
     actual_duration_min = (unix_timestamp(actual_arrival) - unix_timestamp(actual_departure)) / 60.0
     ```

---

### Gold Analytical Marts & Industry Formulations

`pipeline/gold.py` builds business-level marts; formulas follow PostgresPro's sample queries and gonor.me's airline KPI definitions.

1. **`gold_route_revenue`**: monthly revenue by route (`dep_airport`, `arr_airport`), month, and `fare_conditions`. `sum(amount)` and `count(ticket_no)`.
2. **`gold_route_pareto`**: routes ranked by revenue descending, with a windowed cumulative share:
   ```sql
   sum(revenue) OVER (ORDER BY revenue DESC, dep_airport, arr_airport
     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rev
   ```
   `cum_share = running_rev / total_rev`, used to isolate the top 80/20 routes.
3. **`gold_flight_occupancy`**: load factor for completed flights (`status IN ('Departed', 'Arrived')`) = boarded passengers / physical seats, joining `boarding_passes` against `seats`.
4. **`gold_fleet`**: per aircraft code - total flights, flight hours (`sum(actual_duration_min) / 60`), avg duration, avg load factor, seat mix (`seats_economy`, `seats_comfort`, `seats_business`).
5. **`gold_delay_heatmap`**: day-of-week (1=Mon..7=Sun) × local departure hour (0-23), over `Arrived` flights. `is_delayed = dep_delay_min > 15` (standard >15 min threshold). `delay_rate = sum(is_delayed) / total arrived flights`.
6. **`gold_delay_by_aircraft`**: per aircraft model - flight volume, delay count, delay rate, avg delay duration for delayed flights.
7. **`gold_delay_by_route`**: per directional airport pair, same metrics. `HAVING count(*) >= 20` arrived flights to drop low-frequency noise.
8. **Dimensions**: `dim_airports` (geo + departure counts), `dim_routes` (top 100 routes by volume, with coordinates for the map).

---

### Publish Layer, PyMongo Maintenance & Lifecycle Hygiene

`pipeline/publish.py` moves Gold from Iceberg into MongoDB.

1. **Mongo-Spark connector sync**: writes Gold marts with `operationType: "replace"`, `upsertDocument: "true"`, keyed on `_id`. Tags every document with `_run_id`.
2. **Stale record purge**: after connector writes finish, PyMongo runs:
   ```python
   collection.delete_many({"_run_id": {"$ne": current_run_id}})
   ```
   Drops rows/metrics that disappeared in a later recalculation, without dropping the collection.
3. **Secondary indexes**: `month` on `gold_route_revenue`/`gold_flight_occupancy`; `dep_airport` on `gold_route_revenue`/`gold_route_pareto`/`gold_flight_occupancy`/`gold_delay_by_route`/`dim_routes`; `aircraft_code` on `gold_flight_occupancy`/`gold_fleet`/`gold_delay_by_aircraft`.
4. **`pipeline_runs` audit document**:
   ```json
   {
     "_id": "20261004_120000",
     "run_id": "20261004_120000",
     "cutoff": "2017-08-15T18:00:00+00:00",
     "started_at": "2026-10-04T12:00:05.123456+00:00",
     "finished_at": "2026-10-04T12:04:12.789012+00:00",
     "status": "success",
     "counts": {
       "bronze": { "flight_seat_reservations": 5513920, "airport_sites": 104, "aircraft_seat_layouts": 1339 },
       "silver": { "flights_enriched": 65664, ... },
       "gold": { "gold_route_revenue": 3448, ... }
     },
     "quarantine": 0
   }
   ```
5. **Snapshot expiration**: `CALL lake.system.expire_snapshots(table => '...', retain_last => 3)` across Silver and Gold - keeps short-term time-travel without unbounded Parquet growth.
