-- Silver Seats Dimension
CREATE TABLE IF NOT EXISTS lake.silver.seats (
  aircraft_code string,
  seat_no string,
  fare_conditions string
) USING iceberg;

MERGE INTO lake.silver.seats AS target
USING (
  SELECT
    aircraft_code,
    seat_no,
    fare_conditions
  FROM (
    SELECT
      aircraft_code,
      seat_no,
      fare_conditions,
      row_number() OVER (PARTITION BY aircraft_code, seat_no ORDER BY _ingest_ts DESC) AS rn
    FROM lake.bronze.seats
    WHERE _batch_id = '${run_id}'
  )
  WHERE rn = 1
) AS source
ON target.aircraft_code = source.aircraft_code AND target.seat_no = source.seat_no
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
