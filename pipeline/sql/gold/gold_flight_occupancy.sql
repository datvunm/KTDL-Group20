-- Official demo DB sample queries: https://postgrespro.com/docs/postgrespro/9.6/apjs05
-- Metric definitions: https://analytics-flights.gonor.me
CREATE OR REPLACE TABLE lake.gold.gold_flight_occupancy USING iceberg AS
WITH plane_seats AS (
  SELECT aircraft_code, cast(count(*) as int) AS seats
  FROM lake.silver.seats
  GROUP BY aircraft_code
),
flight_boarded AS (
  SELECT flight_id, cast(count(*) as int) AS boarded
  FROM lake.silver.boarding_passes
  GROUP BY flight_id
)
SELECT
  cast(f.flight_id as string) AS _id,
  cast(f.flight_id as int) AS flight_id,
  f.flight_no,
  f.departure_airport AS dep_airport,
  f.arrival_airport AS arr_airport,
  f.aircraft_code,
  f.model,
  date_format(f.scheduled_departure, 'yyyy-MM') AS month,
  date_format(f.scheduled_departure, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS scheduled_departure,
  cast(coalesce(fb.boarded, 0) as int) AS boarded,
  cast(coalesce(ps.seats, 0) as int) AS seats,
  cast(case when coalesce(ps.seats, 0) > 0 then coalesce(fb.boarded, 0) / ps.seats else 0.0 end as double) AS load_factor
FROM lake.silver.flights_enriched f
LEFT JOIN plane_seats ps ON f.aircraft_code = ps.aircraft_code
LEFT JOIN flight_boarded fb ON f.flight_id = fb.flight_id
WHERE f.status IN ('Departed', 'Arrived');
