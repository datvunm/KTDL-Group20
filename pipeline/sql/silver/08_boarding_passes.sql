-- Silver Boarding Passes (from raw flight_seat_reservations rows with a ticket and a seat)
CREATE TABLE IF NOT EXISTS lake.silver.boarding_passes (
  ticket_no string,
  flight_id int,
  boarding_no int,
  seat_no string
) USING iceberg
PARTITIONED BY (bucket(8, flight_id));

-- A checked-in passenger occupies exactly one seat row; empty seats (no ticket)
-- and booked-but-not-checked-in rows (no seat) are not boarding passes
CREATE OR REPLACE TEMPORARY VIEW boarding_passes_batch AS
SELECT
  bp.*,
  CASE
    WHEN bp.n_versions > 1 THEN 'conflicting boarding pass attributes'
    WHEN bp.boarding_no IS NULL THEN 'missing boarding_no'
    WHEN f.flight_id IS NULL THEN 'flight_id not in silver flights'
    WHEN s.seat_no IS NULL THEN 'seat_no not in seats of that flight''s aircraft'
  END AS reject_reason
FROM (
  SELECT *, count(*) OVER (PARTITION BY ticket_no, flight_id) AS n_versions
  FROM (
    SELECT DISTINCT ticket_no, flight_id, boarding_no, seat_no
    FROM lake.bronze.flight_seat_reservations
    WHERE _batch_id = '${run_id}'
      AND ticket_no IS NOT NULL
      AND seat_no IS NOT NULL
  )
) bp
LEFT JOIN lake.silver.flights_enriched f ON bp.flight_id = f.flight_id
LEFT JOIN lake.silver.seats s ON f.aircraft_code = s.aircraft_code AND bp.seat_no = s.seat_no;

INSERT INTO lake.silver.quarantine
SELECT
  'flight_seat_reservations' AS source_table,
  reject_reason AS reason,
  to_json(struct(ticket_no, flight_id, boarding_no, seat_no)) AS payload,
  '${run_id}' AS _batch_id,
  current_timestamp() AS _ingest_ts
FROM boarding_passes_batch
WHERE reject_reason IS NOT NULL;

MERGE INTO lake.silver.boarding_passes AS target
USING (
  SELECT
    ticket_no,
    cast(flight_id as int) AS flight_id,
    cast(boarding_no as int) AS boarding_no,
    seat_no
  FROM boarding_passes_batch
  WHERE reject_reason IS NULL
) AS source
ON target.ticket_no = source.ticket_no AND target.flight_id = source.flight_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
