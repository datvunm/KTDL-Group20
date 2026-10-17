-- Silver Boarding Passes with Quarantine Handling
CREATE TABLE IF NOT EXISTS lake.silver.quarantine (
  source_table string,
  reason string,
  payload string,
  _batch_id string,
  _ingest_ts timestamp
) USING iceberg;

-- Quarantine boarding passes with seat not in aircraft seats
INSERT INTO lake.silver.quarantine
SELECT
  'boarding_passes' AS source_table,
  'seat_no not in seats of that flight''s aircraft' AS reason,
  to_json(struct(
    bp.ticket_no, bp.flight_id, bp.boarding_no, bp.seat_no
  )) AS payload,
  '${run_id}' AS _batch_id,
  current_timestamp() AS _ingest_ts
FROM (
  SELECT *,
    row_number() OVER (PARTITION BY ticket_no, flight_id ORDER BY _ingest_ts DESC) AS rn
  FROM lake.bronze.boarding_passes
  WHERE _batch_id = '${run_id}'
) bp
LEFT JOIN lake.silver.flights_enriched f ON bp.flight_id = f.flight_id
LEFT JOIN lake.silver.seats s ON f.aircraft_code = s.aircraft_code AND bp.seat_no = s.seat_no
WHERE bp.rn = 1
  AND (s.seat_no IS NULL OR f.flight_id IS NULL);

CREATE TABLE IF NOT EXISTS lake.silver.boarding_passes (
  ticket_no string,
  flight_id int,
  boarding_no int,
  seat_no string
) USING iceberg
PARTITIONED BY (bucket(8, flight_id));

MERGE INTO lake.silver.boarding_passes AS target
USING (
  SELECT
    bp.ticket_no,
    bp.flight_id,
    cast(bp.boarding_no as int) AS boarding_no,
    bp.seat_no
  FROM (
    SELECT *,
      row_number() OVER (PARTITION BY ticket_no, flight_id ORDER BY _ingest_ts DESC) AS rn
    FROM lake.bronze.boarding_passes
    WHERE _batch_id = '${run_id}'
  ) bp
  JOIN lake.silver.flights_enriched f ON bp.flight_id = f.flight_id
  JOIN lake.silver.seats s ON f.aircraft_code = s.aircraft_code AND bp.seat_no = s.seat_no
  WHERE bp.rn = 1
) AS source
ON target.ticket_no = source.ticket_no AND target.flight_id = source.flight_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
