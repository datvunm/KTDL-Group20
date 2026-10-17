-- Official demo DB sample queries: https://postgrespro.com/docs/postgrespro/9.6/apjs05
-- Metric definitions: https://analytics-flights.gonor.me
CREATE OR REPLACE TABLE lake.gold.gold_delay_heatmap USING iceberg AS
WITH base AS (
  SELECT
    cast(case when dayofweek(scheduled_departure_local) = 1 then 7 else dayofweek(scheduled_departure_local) - 1 end as int) AS dow,
    cast(hour(scheduled_departure_local) as int) AS hour,
    case when dep_delay_min > 15 then 1 else 0 end AS is_delayed
  FROM lake.silver.flights_enriched
  WHERE status = 'Arrived'
)
SELECT
  concat_ws('|', cast(dow as string), cast(hour as string)) AS _id,
  dow,
  hour,
  cast(count(*) as int) AS flights,
  cast(sum(is_delayed) as int) AS delayed,
  cast(sum(is_delayed) / count(*) as double) AS delay_rate
FROM base
GROUP BY dow, hour;
