# Source: PostgreSQL Airlines Database & Simulator

This component provides the operational data source for the Airlines Lakehouse demo:
1. **Historical Baseline Dump**: Restores the official PostgresPro Airlines demo medium database into schema `archive`.
2. **Operational Schema**: Empty schema `bookings` containing identical 8 operational tables, constraints, and indexes, without foreign keys or views.
3. **Change Simulator**: Deterministic replay tool advancing database state to any cutoff timestamp.

---

## Architecture

- **Database**: `demo` (in service `db`, PostgreSQL 16)
- **Search Path**: `bookings, public` (unqualified queries from Spark JDBC hit `bookings`)
- **Schemas**:
  - `archive`: Untouched full dump baseline (`archive.now()` IMMUTABLE returning `'2017-08-15 18:00:00+03'`).
  - `bookings`: Operational schema populated deterministically by the simulator based on cutoff time.
- **Clock Function**: `bookings.now()` is a `STABLE` SQL function returning the current simulation cutoff from `bookings.sim_state(cutoff timestamptz)`. Being `STABLE` (not `IMMUTABLE`), PostgreSQL query planner never folds stale values.

---

## File Structure

```text
source/
├── load_dump.sh            # Host script to restore dump and initialize demo DB
├── schema.sql              # DDL for bookings schema, sim_state, and bookings.now()
├── simulate.sql            # Transactional simulation SQL parameterized by :cutoff
├── simulate.sh             # CLI runner for simulate.sql printing row counts & status
├── README.md               # Documentation
└── tests/
    ├── synthetic_dump.sql  # Minimal synthetic dump covering all statuses and edge cases
    └── test_simulate.py    # Pytest test suite asserting simulation invariants
```

---

## Simulation Rules

The simulator (`simulate.sql`) executes as a single transaction (`SET LOCAL work_mem = '256MB'`), truncating and deterministically reinserting rows into `bookings` as a pure function of `:cutoff`:

1. **Dimensions**: `aircrafts_data`, `airports_data`, and `seats` are copied fully from `archive`.
2. **Bookings & Tickets**:
   - `bookings`: `archive.bookings` where `book_date <= :cutoff`.
   - `tickets`: tickets belonging to released bookings.
   - `ticket_flights`: flight coupons belonging to released tickets.
3. **Flight Release Rule**:
   - Flights with `scheduled_departure <= :cutoff + interval '31 days'` OR referenced by released `ticket_flights` (e.g. advance bookings booked > 30 days prior).
   - Conceptually inserted before `ticket_flights` and `boarding_passes`.
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
5. **Boarding Passes**:
   - Released tickets whose flight is not Cancelled and whose check-in has opened (`scheduled_departure - interval '24 hours' <= :cutoff`).
6. **Acceptance Criteria**:
   - At final cutoff `'2017-08-15 18:00:00+03'`, row counts of all 8 tables equal `archive`, and `archive.flights EXCEPT bookings.flights` returns 0 rows.

---

## Usage

### 1. Load Baseline Dump

Run from host (any directory):
```bash
./source/load_dump.sh
```
- Automatically downloads and unzips `https://edu.postgrespro.com/demo-medium-en.zip` into `source/data/` if missing.
- Pre-creates `demo` database so dump's internal `DROP DATABASE demo;` succeeds.
- Restores dump into `demo`, renames `bookings` schema to `archive`.
- Executes `source/schema.sql` to create empty operational `bookings` schema and initializes cutoff to `2017-04-01 00:00:00+03`.

### 2. Run Simulation

Advance database state to a specific cutoff:
```bash
# ISO Date
./source/simulate.sh 2017-06-15

# Timestamp with timezone
./source/simulate.sh '2017-08-15 18:00:00+03'
```
The script runs `simulate.sql` and prints table row counts and flight status distribution.

### 3. Environment Overrides

Both scripts support execution overrides:
- `PSQL_EXEC`: Command to invoke `psql` (default: `docker compose -f postgres/docker-compose.dev.yaml exec -T db psql -U postgres`).
- `DUMP_FILE`: Custom dump path (skips download).
- `SCHEMA_FILE`: Custom `schema.sql` path.
- `SIMULATE_SQL`: Custom `simulate.sql` path.

Example running against a local PostgreSQL server:
```bash
PSQL_EXEC="psql -h localhost -p 5432 -U postgres" ./source/simulate.sh 2017-07-01
```

---

## Local Tests

Unit and integration tests run without Docker using `pgserver` (embedded PostgreSQL binary) and synthetic archive data:

```bash
pytest source/tests/test_simulate.py -v
```

Verified assertions:
- `load_dump.sh` execution and idempotency.
- (a) Bookings after cutoff are absent.
- (b) Flight status transitions across cutoffs (Scheduled $\rightarrow$ On Time $\rightarrow$ Departed $\rightarrow$ Arrived) with NULL actuals after cutoff.
- (c) Cancelled flights remain Cancelled with NULL actuals.
- (d) Boarding passes appear only once check-in has opened (within 24 hours).
- (e) At archive final `now()`, every `bookings` table equals `archive` (`EXCEPT` both ways is empty).
- (f) `bookings.now()` returns cutoff and has `provolatile = 's'` (`STABLE`).
- (g) Re-running the same cutoff yields identical row counts.
- Advance bookings scheduled > 30 days after booking are released.
