-- Official demo DB sample queries: https://postgrespro.com/docs/postgrespro/9.6/apjs05
-- Metric definitions: https://analytics-flights.gonor.me
CREATE OR REPLACE TABLE lake.gold.gold_delay_by_aircraft USING iceberg AS
SELECT
  f.aircraft_code AS _id,
  f.aircraft_code,
  coalesce(f.model, ac.model) AS model,
  cast(count(*) as int) AS flights,
  cast(count(case when f.dep_delay_min > 15 then 1 end) as int) AS delayed,
  cast(count(case when f.dep_delay_min > 15 then 1 end) / count(*) as double) AS delay_rate,
  cast(coalesce(avg(case when f.dep_delay_min > 15 then f.dep_delay_min end), 0.0) as double) AS avg_delay_min
FROM lake.silver.flights_enriched f
LEFT JOIN lake.silver.aircrafts ac ON f.aircraft_code = ac.aircraft_code
WHERE f.status = 'Arrived'
GROUP BY f.aircraft_code, coalesce(f.model, ac.model);
