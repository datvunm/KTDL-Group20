-- Silver Aircrafts Dimension
CREATE TABLE IF NOT EXISTS lake.silver.aircrafts (
  aircraft_code string,
  model string,
  range int
) USING iceberg;

MERGE INTO lake.silver.aircrafts AS target
USING (
  SELECT
    aircraft_code,
    model,
    range
  FROM (
    SELECT
      aircraft_code,
      get_json_object(model, '$.en') AS model,
      cast(range as int) AS range,
      row_number() OVER (PARTITION BY aircraft_code ORDER BY _ingest_ts DESC) AS rn
    FROM lake.bronze.aircrafts_data
    WHERE _batch_id = '${run_id}'
  )
  WHERE rn = 1
) AS source
ON target.aircraft_code = source.aircraft_code
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
