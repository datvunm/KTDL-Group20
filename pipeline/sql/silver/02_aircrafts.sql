-- Silver Aircrafts Dimension (from raw aircraft_seat_layouts: one row per seat per model)
CREATE TABLE IF NOT EXISTS lake.silver.aircrafts (
  aircraft_code string,
  model string,
  range int
) USING iceberg;

-- Collapse the seat map to one row per aircraft; an aircraft_code whose
-- model attributes disagree across seat rows is conflicting
CREATE OR REPLACE TEMPORARY VIEW aircrafts_batch AS
SELECT
  *,
  CASE
    WHEN aircraft_code IS NULL THEN 'missing aircraft_code'
    WHEN count(*) OVER (PARTITION BY aircraft_code) > 1 THEN 'conflicting aircraft attributes'
  END AS reject_reason
FROM (
  SELECT DISTINCT aircraft_code, model, range
  FROM lake.bronze.aircraft_seat_layouts
  WHERE _batch_id = '${run_id}'
);

INSERT INTO lake.silver.quarantine
SELECT
  'aircraft_seat_layouts' AS source_table,
  reject_reason AS reason,
  to_json(struct(aircraft_code, model, range)) AS payload,
  '${run_id}' AS _batch_id,
  current_timestamp() AS _ingest_ts
FROM aircrafts_batch
WHERE reject_reason IS NOT NULL;

MERGE INTO lake.silver.aircrafts AS target
USING (
  SELECT
    aircraft_code,
    get_json_object(model, '$.en') AS model,
    cast(range as int) AS range
  FROM aircrafts_batch
  WHERE reject_reason IS NULL
) AS source
ON target.aircraft_code = source.aircraft_code
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
