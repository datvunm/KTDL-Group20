-- Silver Seats Dimension (from raw aircraft_seat_layouts rows that carry a seat)
CREATE TABLE IF NOT EXISTS lake.silver.seats (
  aircraft_code string,
  seat_no string,
  fare_conditions string
) USING iceberg;

-- One row per (aircraft_code, seat_no); a seat listed with more than one
-- fare class is conflicting
CREATE OR REPLACE TEMPORARY VIEW seats_batch AS
SELECT
  *,
  CASE
    WHEN aircraft_code IS NULL THEN 'missing aircraft_code'
    WHEN fare_conditions NOT IN ('Economy', 'Comfort', 'Business') THEN 'invalid fare_conditions'
    WHEN count(*) OVER (PARTITION BY aircraft_code, seat_no) > 1 THEN 'conflicting seat attributes'
  END AS reject_reason
FROM (
  SELECT DISTINCT aircraft_code, seat_no, seats_fare_conditions AS fare_conditions
  FROM lake.bronze.aircraft_seat_layouts
  WHERE _batch_id = '${run_id}'
    AND seat_no IS NOT NULL
);

INSERT INTO lake.silver.quarantine
SELECT
  'aircraft_seat_layouts' AS source_table,
  reject_reason AS reason,
  to_json(struct(aircraft_code, seat_no, fare_conditions)) AS payload,
  '${run_id}' AS _batch_id,
  current_timestamp() AS _ingest_ts
FROM seats_batch
WHERE reject_reason IS NOT NULL;

MERGE INTO lake.silver.seats AS target
USING (
  SELECT aircraft_code, seat_no, fare_conditions
  FROM seats_batch
  WHERE reject_reason IS NULL
) AS source
ON target.aircraft_code = source.aircraft_code AND target.seat_no = source.seat_no
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
