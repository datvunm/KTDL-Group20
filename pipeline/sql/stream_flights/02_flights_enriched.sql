-- Real recorded timestamps, so unlike stream_bookings.py this may update
-- status/actuals on an existing row.
MERGE INTO lake.silver.flights_enriched AS target
USING (
  SELECT
    cast(e.flight_id as int) AS flight_id, e.flight_no, e.scheduled_departure, e.scheduled_arrival,
    e.departure_airport, e.arrival_airport, e.status, e.aircraft_code,
    e.actual_departure, e.actual_arrival,
    dep.airport_name AS departure_airport_name, dep.city AS departure_city,
    dep.lon AS departure_lon, dep.lat AS departure_lat, dep.timezone AS departure_timezone,
    arr.airport_name AS arrival_airport_name, arr.city AS arrival_city,
    arr.lon AS arrival_lon, arr.lat AS arrival_lat, arr.timezone AS arrival_timezone,
    ac.model AS model,
    from_utc_timestamp(e.scheduled_departure, coalesce(dep.timezone, 'UTC')) AS scheduled_departure_local,
    case when e.actual_departure is not null
         then from_utc_timestamp(e.actual_departure, coalesce(dep.timezone, 'UTC')) else null end
         AS actual_departure_local,
    round(2 * 6371 * asin(sqrt(
      pow(sin(radians(arr.lat - dep.lat) / 2), 2) +
      cos(radians(dep.lat)) * cos(radians(arr.lat)) *
      pow(sin(radians(arr.lon - dep.lon) / 2), 2)
    )), 2) AS distance_km,
    cast((unix_timestamp(e.scheduled_arrival) - unix_timestamp(e.scheduled_departure)) / 60.0 as double)
      AS scheduled_duration_min,
    case when e.actual_arrival is not null and e.actual_departure is not null
         then cast((unix_timestamp(e.actual_arrival) - unix_timestamp(e.actual_departure)) / 60.0 as double)
         else null end AS actual_duration_min,
    case when e.actual_departure is not null
         then cast((unix_timestamp(e.actual_departure) - unix_timestamp(e.scheduled_departure)) / 60.0 as double)
         else null end AS dep_delay_min
  FROM flight_events_dedup e
  JOIN lake.silver.airports dep ON e.departure_airport = dep.airport_code
  JOIN lake.silver.airports arr ON e.arrival_airport = arr.airport_code
  JOIN lake.silver.aircrafts ac ON e.aircraft_code = ac.aircraft_code
) AS source
ON target.flight_id = source.flight_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
