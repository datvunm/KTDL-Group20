-- Silver Airports Dimension
CREATE TABLE IF NOT EXISTS lake.silver.airports (
  airport_code string,
  airport_name string,
  city string,
  lon double,
  lat double,
  timezone string
) USING iceberg;

MERGE INTO lake.silver.airports AS target
USING (
  SELECT
    airport_code,
    airport_name,
    city,
    lon,
    lat,
    timezone
  FROM (
    SELECT
      airport_code,
      get_json_object(airport_name, '$.en') AS airport_name,
      get_json_object(city, '$.en') AS city,
      cast(regexp_extract(coordinates, '^\\(([^,]+),\\s*([^)]+)\\)$', 1) as double) AS lon,
      cast(regexp_extract(coordinates, '^\\(([^,]+),\\s*([^)]+)\\)$', 2) as double) AS lat,
      timezone,
      row_number() OVER (PARTITION BY airport_code ORDER BY _ingest_ts DESC) AS rn
    FROM lake.bronze.airports_data
    WHERE _batch_id = '${run_id}'
  )
  WHERE rn = 1
) AS source
ON target.airport_code = source.airport_code
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
