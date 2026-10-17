-- Official demo DB sample queries: https://postgrespro.com/docs/postgrespro/9.6/apjs05
-- Metric definitions: https://analytics-flights.gonor.me
CREATE OR REPLACE TABLE lake.gold.gold_route_revenue USING iceberg AS
SELECT
  concat_ws('|', f.departure_airport, f.arrival_airport, date_format(f.scheduled_departure, 'yyyy-MM'), tf.fare_conditions) AS _id,
  f.departure_airport AS dep_airport,
  f.arrival_airport AS arr_airport,
  f.departure_city AS dep_city,
  f.arrival_city AS arr_city,
  date_format(f.scheduled_departure, 'yyyy-MM') AS month,
  tf.fare_conditions AS fare_conditions,
  cast(sum(tf.amount) as double) AS revenue,
  cast(count(tf.ticket_no) as int) AS tickets
FROM lake.silver.flights_enriched f
JOIN lake.silver.ticket_flights tf ON f.flight_id = tf.flight_id
GROUP BY
  f.departure_airport,
  f.arrival_airport,
  f.departure_city,
  f.arrival_city,
  date_format(f.scheduled_departure, 'yyyy-MM'),
  tf.fare_conditions;
