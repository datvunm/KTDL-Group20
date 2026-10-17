-- Official demo DB sample queries: https://postgrespro.com/docs/postgrespro/9.6/apjs05
-- Metric definitions: https://analytics-flights.gonor.me
CREATE OR REPLACE TABLE lake.gold.gold_fleet USING iceberg AS
WITH seat_counts AS (
  SELECT
    aircraft_code,
    cast(count(*) as int) AS seats_total,
    cast(count(case when fare_conditions = 'Economy' then 1 end) as int) AS seats_economy,
    cast(count(case when fare_conditions = 'Comfort' then 1 end) as int) AS seats_comfort,
    cast(count(case when fare_conditions = 'Business' then 1 end) as int) AS seats_business
  FROM lake.silver.seats
  GROUP BY aircraft_code
),
flight_metrics AS (
  SELECT
    fo.aircraft_code,
    cast(count(fo.flight_id) as int) AS flights,
    cast(coalesce(sum(f.actual_duration_min) / 60.0, 0.0) as double) AS flight_hours,
    cast(coalesce(avg(f.actual_duration_min), 0.0) as double) AS avg_duration_min,
    cast(coalesce(avg(fo.load_factor), 0.0) as double) AS avg_load_factor
  FROM lake.gold.gold_flight_occupancy fo
  JOIN lake.silver.flights_enriched f ON fo.flight_id = f.flight_id
  GROUP BY fo.aircraft_code
)
SELECT
  a.aircraft_code AS _id,
  a.aircraft_code,
  a.model,
  cast(a.range as int) AS range,
  coalesce(sc.seats_total, 0) AS seats_total,
  coalesce(sc.seats_economy, 0) AS seats_economy,
  coalesce(sc.seats_comfort, 0) AS seats_comfort,
  coalesce(sc.seats_business, 0) AS seats_business,
  coalesce(fm.flights, 0) AS flights,
  coalesce(fm.flight_hours, 0.0) AS flight_hours,
  coalesce(fm.avg_duration_min, 0.0) AS avg_duration_min,
  coalesce(fm.avg_load_factor, 0.0) AS avg_load_factor
FROM lake.silver.aircrafts a
LEFT JOIN seat_counts sc ON a.aircraft_code = sc.aircraft_code
LEFT JOIN flight_metrics fm ON a.aircraft_code = fm.aircraft_code;
