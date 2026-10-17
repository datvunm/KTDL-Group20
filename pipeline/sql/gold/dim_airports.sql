-- Official demo DB sample queries: https://postgrespro.com/docs/postgrespro/9.6/apjs05
-- Metric definitions: https://analytics-flights.gonor.me
CREATE OR REPLACE TABLE lake.gold.dim_airports USING iceberg AS
SELECT
  a.airport_code AS _id,
  a.airport_code,
  a.airport_name,
  a.city,
  a.lon,
  a.lat,
  a.timezone,
  cast(count(f.flight_id) as int) AS departures
FROM lake.silver.airports a
LEFT JOIN lake.silver.flights_enriched f ON a.airport_code = f.departure_airport
GROUP BY a.airport_code, a.airport_name, a.city, a.lon, a.lat, a.timezone;
