-- Silver Flights Enriched with Quarantine Handling
CREATE TABLE IF NOT EXISTS lake.silver.quarantine (
  source_table string,
  reason string,
  payload string,
  _batch_id string,
  _ingest_ts timestamp
) USING iceberg;

-- Quarantine bad flights
INSERT INTO lake.silver.quarantine
SELECT
  'flights' AS source_table,
  CASE
    WHEN actual_arrival IS NOT NULL AND actual_departure IS NOT NULL AND actual_arrival <= actual_departure
      THEN 'actual_arrival <= actual_departure'
    WHEN status NOT IN ('On Time', 'Delayed', 'Departed', 'Arrived', 'Scheduled', 'Cancelled')
      THEN 'invalid_status'
    ELSE 'unknown'
  END AS reason,
  to_json(struct(
    flight_id, flight_no, scheduled_departure, scheduled_arrival,
    departure_airport, arrival_airport, status, aircraft_code,
    actual_departure, actual_arrival
  )) AS payload,
  '${run_id}' AS _batch_id,
  current_timestamp() AS _ingest_ts
FROM (
  SELECT *,
    row_number() OVER (PARTITION BY flight_id ORDER BY _ingest_ts DESC) AS rn
  FROM lake.bronze.flights
  WHERE _batch_id = '${run_id}'
)
WHERE rn = 1
  AND (
    (actual_arrival IS NOT NULL AND actual_departure IS NOT NULL AND actual_arrival <= actual_departure)
    OR (status NOT IN ('On Time', 'Delayed', 'Departed', 'Arrived', 'Scheduled', 'Cancelled'))
  );

CREATE TABLE IF NOT EXISTS lake.silver.flights_enriched (
  flight_id int,
  flight_no string,
  scheduled_departure timestamp,
  scheduled_arrival timestamp,
  departure_airport string,
  arrival_airport string,
  status string,
  aircraft_code string,
  actual_departure timestamp,
  actual_arrival timestamp,
  departure_airport_name string,
  departure_city string,
  departure_lon double,
  departure_lat double,
  departure_timezone string,
  arrival_airport_name string,
  arrival_city string,
  arrival_lon double,
  arrival_lat double,
  arrival_timezone string,
  model string,
  scheduled_departure_local timestamp,
  actual_departure_local timestamp,
  distance_km double,
  scheduled_duration_min double,
  actual_duration_min double,
  dep_delay_min double
) USING iceberg
PARTITIONED BY (months(scheduled_departure));

MERGE INTO lake.silver.flights_enriched AS target
USING (
  SELECT
    f.flight_id,
    f.flight_no,
    f.scheduled_departure,
    f.scheduled_arrival,
    f.departure_airport,
    f.arrival_airport,
    f.status,
    f.aircraft_code,
    f.actual_departure,
    f.actual_arrival,
    dep.airport_name AS departure_airport_name,
    dep.city AS departure_city,
    dep.lon AS departure_lon,
    dep.lat AS departure_lat,
    dep.timezone AS departure_timezone,
    arr.airport_name AS arrival_airport_name,
    arr.city AS arrival_city,
    arr.lon AS arrival_lon,
    arr.lat AS arrival_lat,
    arr.timezone AS arrival_timezone,
    ac.model AS model,
    from_utc_timestamp(f.scheduled_departure, coalesce(dep.timezone, 'UTC')) AS scheduled_departure_local,
    case
      when f.actual_departure is not null
      then from_utc_timestamp(f.actual_departure, coalesce(dep.timezone, 'UTC'))
      else null
    end AS actual_departure_local,
    round(2 * 6371 * asin(sqrt(
      pow(sin(radians(arr.lat - dep.lat) / 2), 2) +
      cos(radians(dep.lat)) * cos(radians(arr.lat)) *
      pow(sin(radians(arr.lon - dep.lon) / 2), 2)
    )), 2) AS distance_km,
    cast((unix_timestamp(f.scheduled_arrival) - unix_timestamp(f.scheduled_departure)) / 60.0 as double) AS scheduled_duration_min,
    case
      when f.actual_arrival is not null and f.actual_departure is not null
      then cast((unix_timestamp(f.actual_arrival) - unix_timestamp(f.actual_departure)) / 60.0 as double)
      else null
    end AS actual_duration_min,
    case
      when f.actual_departure is not null
      then cast((unix_timestamp(f.actual_departure) - unix_timestamp(f.scheduled_departure)) / 60.0 as double)
      else null
    end AS dep_delay_min
  FROM (
    SELECT *,
      row_number() OVER (PARTITION BY flight_id ORDER BY _ingest_ts DESC) AS rn
    FROM lake.bronze.flights
    WHERE _batch_id = '${run_id}'
  ) f
  LEFT JOIN lake.silver.airports dep ON f.departure_airport = dep.airport_code
  LEFT JOIN lake.silver.airports arr ON f.arrival_airport = arr.airport_code
  LEFT JOIN lake.silver.aircrafts ac ON f.aircraft_code = ac.aircraft_code
  WHERE f.rn = 1
    AND NOT (
      (f.actual_arrival IS NOT NULL AND f.actual_departure IS NOT NULL AND f.actual_arrival <= f.actual_departure)
      OR (f.status NOT IN ('On Time', 'Delayed', 'Departed', 'Arrived', 'Scheduled', 'Cancelled'))
    )
) AS source
ON target.flight_id = source.flight_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
