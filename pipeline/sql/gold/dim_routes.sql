-- Official demo DB sample queries: https://postgrespro.com/docs/postgrespro/9.6/apjs05
-- Metric definitions: https://analytics-flights.gonor.me
CREATE OR REPLACE TABLE lake.gold.dim_routes USING iceberg AS
SELECT
  concat_ws('|', departure_airport, arrival_airport) AS _id,
  departure_airport AS dep_airport,
  arrival_airport AS arr_airport,
  departure_lon AS dep_lon,
  departure_lat AS dep_lat,
  arrival_lon AS arr_lon,
  arrival_lat AS arr_lat,
  cast(count(*) as int) AS flights
FROM lake.silver.flights_enriched
GROUP BY
  departure_airport,
  arrival_airport,
  departure_lon,
  departure_lat,
  arrival_lon,
  arrival_lat
ORDER BY count(*) DESC
LIMIT 100;
