CREATE TABLE IF NOT EXISTS lake.silver.flights_enriched (
  flight_id int, flight_no string, scheduled_departure timestamp, scheduled_arrival timestamp,
  departure_airport string, arrival_airport string, status string, aircraft_code string,
  actual_departure timestamp, actual_arrival timestamp,
  departure_airport_name string, departure_city string, departure_lon double, departure_lat double,
  departure_timezone string,
  arrival_airport_name string, arrival_city string, arrival_lon double, arrival_lat double,
  arrival_timezone string,
  model string, scheduled_departure_local timestamp, actual_departure_local timestamp,
  distance_km double, scheduled_duration_min double, actual_duration_min double, dep_delay_min double
) USING iceberg PARTITIONED BY (months(scheduled_departure));

-- Insert-only: creates a flight if the batch pipeline hasn't loaded it yet.
-- Never overwrites status/actuals - that's stream_flights.py's job.
MERGE INTO lake.silver.flights_enriched AS target
USING (
  SELECT DISTINCT
    cast(s.flight_id as int) AS flight_id, s.flight_no, s.scheduled_departure, s.scheduled_arrival,
    s.departure_airport, s.arrival_airport, 'Scheduled' AS status, s.aircraft_code,
    cast(NULL as timestamp) AS actual_departure, cast(NULL as timestamp) AS actual_arrival,
    dep.airport_name AS departure_airport_name, dep.city AS departure_city,
    dep.lon AS departure_lon, dep.lat AS departure_lat, dep.timezone AS departure_timezone,
    arr.airport_name AS arrival_airport_name, arr.city AS arrival_city,
    arr.lon AS arrival_lon, arr.lat AS arrival_lat, arr.timezone AS arrival_timezone,
    ac.model AS model,
    from_utc_timestamp(s.scheduled_departure, coalesce(dep.timezone, 'UTC')) AS scheduled_departure_local,
    cast(NULL as timestamp) AS actual_departure_local,
    round(2 * 6371 * asin(sqrt(
      pow(sin(radians(arr.lat - dep.lat) / 2), 2) +
      cos(radians(dep.lat)) * cos(radians(arr.lat)) *
      pow(sin(radians(arr.lon - dep.lon) / 2), 2)
    )), 2) AS distance_km,
    cast((unix_timestamp(s.scheduled_arrival) - unix_timestamp(s.scheduled_departure)) / 60.0 as double)
      AS scheduled_duration_min,
    cast(NULL as double) AS actual_duration_min,
    cast(NULL as double) AS dep_delay_min
  FROM segments_batch s
  JOIN lake.silver.airports dep ON s.departure_airport = dep.airport_code
  JOIN lake.silver.airports arr ON s.arrival_airport = arr.airport_code
  JOIN lake.silver.aircrafts ac ON s.aircraft_code = ac.aircraft_code
  WHERE s.flight_id IS NOT NULL
) AS source
ON target.flight_id = source.flight_id
WHEN NOT MATCHED THEN INSERT *;
