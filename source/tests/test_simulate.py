# source/tests/test_simulate.py
"""
Test suite for Source slice: schema.sql, simulate.sql, simulate.sh, and load_dump.sh.
Verifies all contract and simulator requirements on synthetic raw archive data without Docker.
"""

import gzip
import os
import shutil
import subprocess
import tempfile
import pytest

try:
    import pgserver
    import psycopg2
    HAS_PGSERVER = True
except ImportError:
    HAS_PGSERVER = False

pytestmark = pytest.mark.skipif(not HAS_PGSERVER, reason="pgserver and psycopg2 are required")

SOURCE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SYNTHETIC_DUMP = os.path.join(SOURCE_DIR, "tests", "synthetic_dump.sql")
SCHEMA_SQL = os.path.join(SOURCE_DIR, "schema.sql")
SIMULATE_SQL = os.path.join(SOURCE_DIR, "simulate.sql")
SIMULATE_SH = os.path.join(SOURCE_DIR, "simulate.sh")
LOAD_DUMP_SH = os.path.join(SOURCE_DIR, "load_dump.sh")

# coordinates is cast to text: point has no default B-tree equality operator
ALL_TABLES = {
    "flight_seat_reservations": "*",
    "airport_sites": "airport_code, airport_name, city, coordinates::text, timezone",
    "aircraft_seat_layouts": "*",
}

FINAL_CUTOFF = "2017-08-15 18:00:00+03"


@pytest.fixture(scope="module")
def pg_cluster():
    """Starts a standalone local PostgreSQL instance via pgserver for the test module."""
    pgdata = tempfile.mkdtemp(prefix="ktdl-src-test-pg-")
    pg = pgserver.get_server(pgdata)
    bindir = os.path.join(os.path.dirname(pgserver.__file__), "pginstall", "bin")
    psql_bin = os.path.join(bindir, "psql")

    # Package the synthetic dump like source/data/raw: gzip parts in a directory
    dump_dir = tempfile.mkdtemp(prefix="ktdl-src-test-dump-")
    with open(SYNTHETIC_DUMP, "rb") as src, gzip.open(
        os.path.join(dump_dir, "synthetic.sql.gz.part-00"), "wb"
    ) as dst:
        shutil.copyfileobj(src, dst)

    env = os.environ.copy()
    env["PSQL_EXEC"] = f"{psql_bin} -h {pgdata} -U postgres"
    env["DUMP_DIR"] = dump_dir
    env["SCHEMA_FILE"] = SCHEMA_SQL
    env["SIMULATE_SQL"] = SIMULATE_SQL

    # Run load_dump.sh initially to initialize demo DB
    res = subprocess.run(["bash", LOAD_DUMP_SH], env=env, capture_output=True, text=True)
    assert res.returncode == 0, f"load_dump.sh failed: {res.stderr}\n{res.stdout}"

    yield {
        "pgdata": pgdata,
        "psql_bin": psql_bin,
        "env": env,
        "uri": pg.get_uri("demo"),
    }

    pg.cleanup()
    shutil.rmtree(pgdata, ignore_errors=True)
    shutil.rmtree(dump_dir, ignore_errors=True)


def query_demo(pg_cluster, sql: str, fetch: str = "all"):
    """Runs a query on demo database with autocommit=True so locks are released immediately."""
    with psycopg2.connect(pg_cluster["uri"]) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
            if fetch == "one":
                return cur.fetchone()
            elif fetch == "val":
                row = cur.fetchone()
                return row[0] if row else None
            return cur.fetchall()


def run_simulate(pg_cluster, cutoff: str) -> subprocess.CompletedProcess:
    """Executes simulate.sql for a given cutoff."""
    cmd = [
        pg_cluster["psql_bin"],
        "-h", pg_cluster["pgdata"],
        "-U", "postgres",
        "-d", "demo",
        "-v", "ON_ERROR_STOP=1",
        "-v", f"cutoff='{cutoff}'",
        "-f", SIMULATE_SQL,
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"simulate.sql failed for cutoff '{cutoff}': {res.stderr}\n{res.stdout}"
    return res


def flight_state(pg_cluster, flight_id: int):
    """Returns the distinct (status, actual_departure, actual_arrival) rows of a flight."""
    return query_demo(
        pg_cluster,
        f"""
        SELECT DISTINCT status, actual_departure, actual_arrival
        FROM raw.flight_seat_reservations WHERE flight_id = {flight_id};
        """,
    )


def test_scripts_syntax():
    """Verify bash script syntax with bash -n."""
    assert subprocess.run(["bash", "-n", LOAD_DUMP_SH]).returncode == 0
    assert subprocess.run(["bash", "-n", SIMULATE_SH]).returncode == 0


def test_load_dump_sh_execution(pg_cluster):
    """
    Asserts load_dump.sh executes idempotently, recreates demo, restores the raw
    dump parts into archive, creates the empty raw schema, and sets up initial sim_state.
    """
    res = subprocess.run(["bash", LOAD_DUMP_SH], env=pg_cluster["env"], capture_output=True, text=True)
    assert res.returncode == 0, f"load_dump.sh re-run failed: {res.stderr}\n{res.stdout}"
    assert "Database demo initialized successfully" in res.stdout

    # archive holds the 14 synthetic rows over 6 flights; raw is initially empty
    arch_rows, arch_flights = query_demo(
        pg_cluster,
        "SELECT count(*), count(DISTINCT flight_id) FROM archive.flight_seat_reservations;",
        fetch="one",
    )
    assert (arch_rows, arch_flights) == (14, 6)

    raw_rows = query_demo(pg_cluster, "SELECT count(*) FROM raw.flight_seat_reservations;", fetch="val")
    assert raw_rows == 0

    # Compare as timestamptz: the text form depends on the session TimeZone
    assert query_demo(pg_cluster, "SELECT raw.now() = '2017-04-01 00:00:00+03'::timestamptz;", fetch="val")


def test_a_booking_after_cutoff_is_absent(pg_cluster):
    """
    Requirement (a): a booking after the cutoff is absent.
    Cutoff: 2017-05-05.
    Booking B00001 is on 2017-05-01 (<= cutoff).
    Bookings B00002..B00005 are in June-August (> cutoff).
    """
    run_simulate(pg_cluster, "2017-05-05 00:00:00+03")

    refs = [
        row[0]
        for row in query_demo(
            pg_cluster,
            "SELECT DISTINCT book_ref FROM raw.flight_seat_reservations WHERE book_ref IS NOT NULL;",
        )
    ]
    assert refs == ["B00001"]

    # Tickets of absent bookings must also be absent
    tickets = query_demo(
        pg_cluster,
        "SELECT ticket_no, book_ref FROM raw.flight_seat_reservations WHERE ticket_no IS NOT NULL;",
    )
    assert len(tickets) == 1
    assert tickets[0][1] == "B00001"


def test_b_flight_status_transitions_and_null_actuals(pg_cluster):
    """
    Requirement (b): a flight that has Arrived in the archive shows as
    Scheduled / On Time / Departed at the matching earlier cutoffs, with NULL actuals after the cutoff.
    Flight 1 in archive:
      scheduled: 2017-06-10 10:00 -> 11:30
      actuals:   2017-06-10 10:00 -> 11:30
      status:    Arrived
    """
    # 1. > 24h before scheduled departure: Scheduled, actuals NULL
    run_simulate(pg_cluster, "2017-06-01 00:00:00+03")
    assert flight_state(pg_cluster, 1) == [("Scheduled", None, None)]

    # 2. Within 24h before scheduled departure: On Time, actuals NULL
    run_simulate(pg_cluster, "2017-06-09 15:00:00+03")
    assert flight_state(pg_cluster, 1) == [("On Time", None, None)]

    # 3. After departure, before arrival: Departed, actual_dep NOT NULL, actual_arr NULL
    run_simulate(pg_cluster, "2017-06-10 10:30:00+03")
    [(status, act_dep, act_arr)] = flight_state(pg_cluster, 1)
    assert status == "Departed"
    assert act_dep is not None
    assert act_arr is None

    # 4. After arrival: Arrived, both actuals NOT NULL
    run_simulate(pg_cluster, "2017-06-10 12:00:00+03")
    [(status, act_dep, act_arr)] = flight_state(pg_cluster, 1)
    assert status == "Arrived"
    assert act_dep is not None
    assert act_arr is not None


def test_c_cancelled_stays_cancelled(pg_cluster):
    """
    Requirement (c): Cancelled stays Cancelled at all cutoffs, with NULL actuals.
    Flight 6 is Cancelled (scheduled 2017-07-01 10:00).
    """
    for cutoff in ["2017-06-25 00:00:00+03", "2017-07-01 12:00:00+03", FINAL_CUTOFF]:
        run_simulate(pg_cluster, cutoff)
        assert flight_state(pg_cluster, 6) == [("Cancelled", None, None)]


def test_d_boarding_passes_appear_only_once_checkin_opened(pg_cluster):
    """
    Requirement (d): boarding passes (seated booked rows) appear only once check-in has opened
    (scheduled_departure - 24 hours <= cutoff) and flight not Cancelled.
    Flight 1: scheduled 2017-06-10 10:00. Check-in opens 2017-06-09 10:00.
    """
    seated_sql = (
        "SELECT count(*) FROM raw.flight_seat_reservations "
        "WHERE flight_id = {fid} AND ticket_no IS NOT NULL AND seat_no IS NOT NULL;"
    )

    # 25 hours before flight: check-in not open -> 0 boarding passes
    run_simulate(pg_cluster, "2017-06-09 09:00:00+03")
    assert query_demo(pg_cluster, seated_sql.format(fid=1), fetch="val") == 0

    # 23 hours before flight: check-in open -> boarding pass released
    run_simulate(pg_cluster, "2017-06-09 11:00:00+03")
    assert query_demo(pg_cluster, seated_sql.format(fid=1), fetch="val") == 1

    # Cancelled flight 6: check-in never opens, 0 boarding passes
    run_simulate(pg_cluster, FINAL_CUTOFF)
    assert query_demo(pg_cluster, seated_sql.format(fid=6), fetch="val") == 0


def test_seat_row_splits_until_checkin_opens(pg_cluster):
    """
    Before check-in opens, the archive row (seat 2A + ticket of B00001) becomes
    an empty seat row plus a booked row without seat; afterwards it is one seated row.
    """
    rows_sql = """
        SELECT seat_no, boarding_no, ticket_no
        FROM raw.flight_seat_reservations WHERE flight_id = 1
        ORDER BY seat_no NULLS LAST;
    """

    run_simulate(pg_cluster, "2017-06-09 09:00:00+03")
    assert query_demo(pg_cluster, rows_sql) == [
        ("1A", None, None),
        ("2A", None, None),
        (None, None, "0005432000001"),
    ]

    run_simulate(pg_cluster, "2017-06-09 11:00:00+03")
    assert query_demo(pg_cluster, rows_sql) == [
        ("1A", None, None),
        ("2A", 1, "0005432000001"),
    ]


def test_e_final_cutoff_equals_archive(pg_cluster):
    """
    Requirement (e): at archive's final now(), every raw table equals archive
    (EXCEPT ALL both ways is empty).
    """
    run_simulate(pg_cluster, FINAL_CUTOFF)

    for tbl, cols in ALL_TABLES.items():
        # archive EXCEPT ALL raw must be 0
        diff1 = query_demo(
            pg_cluster,
            f"SELECT count(*) FROM (SELECT {cols} FROM archive.{tbl} EXCEPT ALL SELECT {cols} FROM raw.{tbl}) q;",
            fetch="val",
        )
        assert diff1 == 0, f"archive.{tbl} EXCEPT ALL raw.{tbl} returned {diff1} rows"

        # raw EXCEPT ALL archive must be 0
        diff2 = query_demo(
            pg_cluster,
            f"SELECT count(*) FROM (SELECT {cols} FROM raw.{tbl} EXCEPT ALL SELECT {cols} FROM archive.{tbl}) q;",
            fetch="val",
        )
        assert diff2 == 0, f"raw.{tbl} EXCEPT ALL archive.{tbl} returned {diff2} rows"


def test_f_raw_now_is_stable_and_returns_cutoff(pg_cluster):
    """
    Requirement (f): raw.now() returns the cutoff and is STABLE (provolatile='s').
    """
    cutoff = "2017-07-01 12:00:00+03"
    run_simulate(pg_cluster, cutoff)

    # Check function returns exact cutoff
    assert query_demo(pg_cluster, f"SELECT raw.now() = '{cutoff}'::timestamptz;", fetch="val")

    # Check pg_proc volatility: 's' = STABLE ('i' = IMMUTABLE, 'v' = VOLATILE)
    volatility = query_demo(
        pg_cluster,
        """
        SELECT provolatile
        FROM pg_proc
        WHERE proname = 'now' AND pronamespace = 'raw'::regnamespace;
        """,
        fetch="val",
    )
    assert volatility == "s", f"Expected provolatile='s', got '{volatility}'"


def test_g_idempotent_rerun_gives_identical_counts(pg_cluster):
    """
    Requirement (g): re-running the same cutoff gives identical counts.
    """
    cutoff = "2017-07-15 00:00:00+03"

    run_simulate(pg_cluster, cutoff)
    counts_run1 = {
        tbl: query_demo(pg_cluster, f"SELECT count(*) FROM raw.{tbl};", fetch="val")
        for tbl in ALL_TABLES
    }

    # Run again for the exact same cutoff
    run_simulate(pg_cluster, cutoff)
    counts_run2 = {
        tbl: query_demo(pg_cluster, f"SELECT count(*) FROM raw.{tbl};", fetch="val")
        for tbl in ALL_TABLES
    }

    assert counts_run1 == counts_run2, f"Rerun counts differed: {counts_run1} != {counts_run2}"


def test_flight_more_than_30_days_after_booking(pg_cluster):
    """
    Tests rule: flight scheduled > 30 days after booking is released via its booking.
    Booking B00001 (2017-05-01) -> Flight 1 scheduled 2017-06-10 (40 days later).
    At cutoff 2017-05-05:
      scheduled_departure <= cutoff + 31 days (2017-06-05) is FALSE.
      Flight 1 is released because it is booked by released booking B00001.
    Flight 6 (2017-07-01, booked by B00004 on 2017-06-20) is not released yet.
    """
    run_simulate(pg_cluster, "2017-05-05 00:00:00+03")
    flights = query_demo(
        pg_cluster,
        "SELECT DISTINCT flight_id, flight_no FROM raw.flight_seat_reservations ORDER BY flight_id;",
    )
    assert flights == [(1, "PG0001")]


def test_simulate_sh_cli_output(pg_cluster):
    """
    Tests simulate.sh invocation from shell and printed summaries.
    """
    res = subprocess.run(["bash", SIMULATE_SH, "2017-06-15"], env=pg_cluster["env"], capture_output=True, text=True)
    assert res.returncode == 0, f"simulate.sh failed: {res.stderr}\n{res.stdout}"
    assert "=== Table Row Counts ===" in res.stdout
    assert "=== Reservation Rows ===" in res.stdout
    assert "=== Flight Status Distribution ===" in res.stdout
    assert "flight_seat_reservations" in res.stdout
    assert "aircraft_seat_layouts" in res.stdout
