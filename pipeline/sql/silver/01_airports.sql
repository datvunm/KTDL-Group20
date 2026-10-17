-- Silver Airports Dimension (from raw airport_sites: one row per airport)
CREATE TABLE IF NOT EXISTS lake.silver.airports (
  airport_code string,
  airport_name string,
  city string,
  lon double,
  lat double,
  timezone string
) USING iceberg;

-- Distinct airport rows of this batch; an airport_code with more than one
-- distinct attribute set is conflicting and goes to quarantine
CREATE OR REPLACE TEMPORARY VIEW airports_batch AS
SELECT
  *,
  CASE
    WHEN airport_code IS NULL THEN 'missing airport_code'
    WHEN count(*) OVER (PARTITION BY airport_code) > 1 THEN 'conflicting airport attributes'
  END AS reject_reason
FROM (
  SELECT DISTINCT airport_code, airport_name, city, coordinates, timezone
  FROM lake.bronze.airport_sites
  WHERE _batch_id = '${run_id}'
);

INSERT INTO lake.silver.quarantine
SELECT
  'airport_sites' AS source_table,
  reject_reason AS reason,
  to_json(struct(airport_code, airport_name, city, coordinates, timezone)) AS payload,
  '${run_id}' AS _batch_id,
  current_timestamp() AS _ingest_ts
FROM airports_batch
WHERE reject_reason IS NOT NULL;

MERGE INTO lake.silver.airports AS target
USING (
  SELECT
    airport_code,
    get_json_object(airport_name, '$.en') AS airport_name,
    get_json_object(city, '$.en') AS city,
    cast(regexp_extract(coordinates, '^\\(([^,]+),\\s*([^)]+)\\)$', 1) as double) AS lon,
    cast(regexp_extract(coordinates, '^\\(([^,]+),\\s*([^)]+)\\)$', 2) as double) AS lat,
    timezone
  FROM airports_batch
  WHERE reject_reason IS NULL
) AS source
ON target.airport_code = source.airport_code
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
