-- Official demo DB sample queries: https://postgrespro.com/docs/postgrespro/9.6/apjs05
-- Metric definitions: https://analytics-flights.gonor.me
CREATE OR REPLACE TABLE lake.gold.gold_route_pareto USING iceberg AS
WITH route_rev AS (
  SELECT
    f.departure_airport AS dep_airport,
    f.arrival_airport AS arr_airport,
    f.departure_city AS dep_city,
    f.arrival_city AS arr_city,
    cast(sum(tf.amount) as double) AS revenue
  FROM lake.silver.flights_enriched f
  JOIN lake.silver.ticket_flights tf ON f.flight_id = tf.flight_id
  GROUP BY f.departure_airport, f.arrival_airport, f.departure_city, f.arrival_city
),
ranked AS (
  SELECT
    dep_airport,
    arr_airport,
    dep_city,
    arr_city,
    revenue,
    cast(row_number() OVER (ORDER BY revenue DESC, dep_airport, arr_airport) as int) AS rank,
    sum(revenue) OVER () AS total_rev,
    sum(revenue) OVER (ORDER BY revenue DESC, dep_airport, arr_airport ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rev
  FROM route_rev
)
SELECT
  concat_ws('|', dep_airport, arr_airport) AS _id,
  dep_airport,
  arr_airport,
  dep_city,
  arr_city,
  revenue,
  rank,
  cast(case when total_rev > 0 then running_rev / total_rev else 0.0 end as double) AS cum_share,
  cast(case when total_rev > 0 then revenue / total_rev else 0.0 end as double) AS revenue_share
FROM ranked;
