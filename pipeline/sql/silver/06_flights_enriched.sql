-- Silver Flights Enriched (from raw flight_seat_reservations: every row carries its flight)
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

-- A flight repeats on every seat row and unseated booking row; collapse to one
-- row per flight_id and validate it against the reference dimensions
CREATE OR REPLACE TEMPORARY VIEW flights_batch AS
SELECT
  f.*,
  CASE
    WHEN f.n_versions > 1 THEN 'conflicting flight attributes'
    WHEN f.actual_arrival IS NOT NULL AND f.actual_departure IS NOT NULL AND f.actual_arrival <= f.actual_departure
      THEN 'actual_arrival <= actual_departure'
    WHEN f.status IS NULL OR f.status NOT IN ('On Time', 'Delayed', 'Departed', 'Arrived', 'Scheduled', 'Cancelled')
      THEN 'invalid_status'
    WHEN dep.airport_code IS NULL THEN 'departure_airport not in silver airports'
    WHEN arr.airport_code IS NULL THEN 'arrival_airport not in silver airports'
    WHEN ac.aircraft_code IS NULL THEN 'aircraft_code not in silver aircrafts'
  END AS reject_reason
FROM (
  SELECT *, count(*) OVER (PARTITION BY flight_id) AS n_versions
  FROM (
    SELECT DISTINCT
      flight_id, flight_no, scheduled_departure, scheduled_arrival,
      departure_airport, arrival_airport, status, aircraft_code,
      actual_departure, actual_arrival
    FROM lake.bronze.flight_seat_reservations
    WHERE _batch_id = '${run_id}'
      AND flight_id IS NOT NULL
  )
) f
LEFT JOIN lake.silver.airports dep ON f.departure_airport = dep.airport_code
LEFT JOIN lake.silver.airports arr ON f.arrival_airport = arr.airport_code
LEFT JOIN lake.silver.aircrafts ac ON f.aircraft_code = ac.aircraft_code;

INSERT INTO lake.silver.quarantine
SELECT
  'flight_seat_reservations' AS source_table,
  reject_reason AS reason,
  to_json(struct(
    flight_id, flight_no, scheduled_departure, scheduled_arrival,
    departure_airport, arrival_airport, status, aircraft_code,
    actual_departure, actual_arrival
  )) AS payload,
  '${run_id}' AS _batch_id,
  current_timestamp() AS _ingest_ts
FROM flights_batch
WHERE reject_reason IS NOT NULL;

MERGE INTO lake.silver.flights_enriched AS target
USING (
  SELECT
    cast(f.flight_id as int) AS flight_id,
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
  FROM flights_batch f
  JOIN lake.silver.airports dep ON f.departure_airport = dep.airport_code
  JOIN lake.silver.airports arr ON f.arrival_airport = arr.airport_code
  JOIN lake.silver.aircrafts ac ON f.aircraft_code = ac.aircraft_code
  WHERE f.reject_reason IS NULL
) AS source
ON target.flight_id = source.flight_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
