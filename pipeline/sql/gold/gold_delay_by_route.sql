-- Official demo DB sample queries: https://postgrespro.com/docs/postgrespro/9.6/apjs05
-- Metric definitions: https://analytics-flights.gonor.me
CREATE OR REPLACE TABLE lake.gold.gold_delay_by_route USING iceberg AS
SELECT
  concat_ws('|', departure_airport, arrival_airport) AS _id,
  departure_airport AS dep_airport,
  arrival_airport AS arr_airport,
  departure_city AS dep_city,
  arrival_city AS arr_city,
  cast(count(*) as int) AS flights,
  cast(count(case when dep_delay_min > 15 then 1 end) as int) AS delayed,
  cast(count(case when dep_delay_min > 15 then 1 end) / count(*) as double) AS delay_rate,
  cast(coalesce(avg(case when dep_delay_min > 15 then dep_delay_min end), 0.0) as double) AS avg_delay_min
FROM lake.silver.flights_enriched
WHERE status = 'Arrived'
GROUP BY departure_airport, arrival_airport, departure_city, arrival_city
HAVING count(*) >= 20;
