# Source: PostgreSQL Airlines Raw Data & Simulator

This component provides the operational data source for the Airlines Lakehouse demo:
1. **Raw Historical Dump**: The official PostgresPro Airlines demo medium database (`demo-medium-en-20170815`), already converted into 3 denormalized raw tables and committed to git. It is restored into schema `archive`.
2. **Raw Schema**: Schema `raw` with the same 3 tables, read by the Bronze layer.
3. **Change Simulator**: Deterministic replay tool filling `raw` with the state of the data as of any cutoff timestamp.

---

## Architecture

- **Database**: `demo` (in service `db`, PostgreSQL 16)
- **Search Path**: `raw, public`
- **Schemas**:
  - `archive`: Full history (data as of `2017-08-15 18:00:00+03`) of the 3 raw tables, restored from `data/raw/`.
  - `raw`: The same 3 tables populated deterministically by the simulator based on cutoff time.
- **Raw tables**:
  - `flight_seat_reservations`: one row per seat per flight (passenger columns NULL when empty), plus one row per booked passenger without a seat yet (seat columns NULL).
  - `airport_sites`: one row per airport.
  - `aircraft_seat_layouts`: one row per seat per aircraft model.
- **Clock Function**: `raw.now()` is a `STABLE` SQL function returning the current simulation cutoff from `raw.sim_state(cutoff timestamptz)`. Being `STABLE` (not `IMMUTABLE`), PostgreSQL query planner never folds stale values.

---

## File Structure

```text
source/
├── data/
│   └── raw/                # Committed raw dump: demo-medium-en-20170815-raw.sql.gz.part-NN + SHA256SUMS
├── load_dump.sh            # Host script to restore the raw dump and initialize demo DB
├── schema.sql              # DDL for raw schema, sim_state, and raw.now()
├── simulate.sql            # Transactional simulation SQL parameterized by :cutoff
├── simulate.sh             # CLI runner for simulate.sql printing row counts & status
├── build_raw.sql           # Conversion of the 8 normalized tables into the 3 raw tables (+ validation, recovery test)
├── export_raw_dump.sh      # One-time tool regenerating data/raw/ from the official dump
├── README.md               # Documentation
└── tests/
    ├── synthetic_dump.sql  # Minimal synthetic raw dump covering all statuses and edge cases
    └── test_simulate.py    # Pytest test suite asserting simulation invariants
```

---

## Raw Dump

`data/raw/` holds the full-history raw tables as one gzip stream split into parts below GitHub's 100 MB file limit (about 242 MB compressed, 1.2 GB SQL). They were produced once by:

```bash
./source/export_raw_dump.sh
```

which downloads the official dump (`https://edu.postgrespro.com/demo-medium-en.zip`, ignored by git), restores it into a scratch database, runs `build_raw.sql` (its validation and recovery test must report 0 lost / 0 extra rows for all 8 original tables plus the route timetable), dumps the 3 tables as schema `archive`, compresses and splits them, and writes `SHA256SUMS`. It only needs to be re-run if `build_raw.sql` changes.

At the final archive state, `flight_seat_reservations` holds **5,513,920** rows (2,360,335 booked + 3,153,585 empty seats).

---

## Simulation Rules

The simulator (`simulate.sql`) executes as a single transaction (`SET LOCAL work_mem = '256MB'`), truncating and deterministically reinserting rows into `raw` as a pure function of `:cutoff`. The rules are the original 8-table rules, applied to the raw rows:

1. **Reference tables**: `airport_sites` and `aircraft_seat_layouts` are copied fully from `archive`.
2. **Bookings**: a booking (and its ticket and flight segment columns) is released if `book_date <= :cutoff`; otherwise those columns disappear.
3. **Flight Release Rule**:
   - Flights with `scheduled_departure <= :cutoff + interval '31 days'` OR booked by a released booking (e.g. advance bookings booked > 30 days prior).
   - Every seat of a released flight gets a row.
4. **Flight State & Actuals as of Cutoff**:
   - `a.status = 'Cancelled'`: Status stays `'Cancelled'`, actuals NULL.
   - `actual_departure`: `a.actual_departure` if `<= :cutoff`, else NULL.
   - `actual_arrival`: `a.actual_arrival` if `<= :cutoff` and departure kept, else NULL.
   - `status`:
     - `'Arrived'` if `actual_arrival IS NOT NULL`
     - `'Departed'` if `actual_departure IS NOT NULL`
     - Else if `scheduled_departure - interval '24 hours' <= :cutoff`:
       - `'Delayed'` if `a.status = 'Delayed' OR a.actual_departure > scheduled_departure`
       - Else `'On Time'`
     - Else `'Scheduled'`
   - `days_of_week` / `duration` (route timetable) are recomputed over the released flights.
5. **Boarding Passes (seat assignment)**:
   - A released passenger sits on their archive seat (with `boarding_no`) only once the flight is not Cancelled and check-in has opened (`scheduled_departure - interval '24 hours' <= :cutoff`).
   - Before that, the archive row splits into an empty seat row and a booked row without seat.
6. **Acceptance Criteria**:
   - At final cutoff `'2017-08-15 18:00:00+03'`, all 3 `raw` tables equal `archive` (`EXCEPT ALL` both ways is empty).
   - At every cutoff, `raw` equals the output of the original pipeline (8-table simulator + `build_raw.sql`); this was verified at 2017-05-01, 2017-05-20 12:00, 2017-06-15, 2017-07-15 and the final cutoff.

---

## Usage

### 1. Load Raw Dump

Run from host (any directory):
```bash
./source/load_dump.sh
```
- Verifies `data/raw/SHA256SUMS`.
- Recreates database `demo` and streams the raw dump parts (`cat | gunzip | psql`) into schema `archive`.
- Executes `source/schema.sql` to create the empty `raw` schema and initializes cutoff to `2017-04-01 00:00:00+03`.

### 2. Run Simulation

Advance database state to a specific cutoff:
```bash
# ISO Date
./source/simulate.sh 2017-06-15

# Timestamp with timezone
./source/simulate.sh '2017-08-15 18:00:00+03'
```
The script runs `simulate.sql` and prints table row counts, booked / seated / unseated / empty seat rows and the flight status distribution.

### 3. Environment Overrides

Both scripts support execution overrides. All paths are host paths: the scripts stream SQL and dump files to `psql` on stdin, so nothing depends on the container's `/source` mount (and Git Bash on Windows cannot rewrite container paths).
- `PSQL_EXEC`: Command to invoke `psql` (default: `docker compose -f postgres/docker-compose.dev.yaml exec -T db psql -U postgres`).
- `DUMP_DIR`: Custom directory of raw dump parts (`*.sql.gz.part-*`).
- `SCHEMA_FILE`: Custom `schema.sql` path.
- `SIMULATE_SQL`: Custom `simulate.sql` path.

Example running against a local PostgreSQL server:
```bash
PSQL_EXEC="psql -h localhost -p 5432 -U postgres" ./source/simulate.sh 2017-07-01
```

---

## Local Tests

Unit and integration tests run without Docker using `pgserver` (embedded PostgreSQL binary) and synthetic raw archive data:

```bash
pytest source/tests/test_simulate.py -v
```

Verified assertions:
- `load_dump.sh` execution and idempotency.
- (a) Bookings after cutoff are absent.
- (b) Flight status transitions across cutoffs (Scheduled $\rightarrow$ On Time $\rightarrow$ Departed $\rightarrow$ Arrived) with NULL actuals after cutoff.
- (c) Cancelled flights remain Cancelled with NULL actuals.
- (d) Boarding passes appear only once check-in has opened (within 24 hours).
- Seat rows split into empty seat + unseated booking until check-in opens.
- (e) At archive final `now()`, every `raw` table equals `archive` (`EXCEPT ALL` both ways is empty).
- (f) `raw.now()` returns cutoff and has `provolatile = 's'` (`STABLE`).
- (g) Re-running the same cutoff yields identical row counts.
- Advance bookings scheduled > 30 days after booking are released.
