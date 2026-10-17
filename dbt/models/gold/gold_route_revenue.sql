{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

-- Official demo DB sample queries: https://postgrespro.com/docs/postgrespro/9.6/apjs05
-- Metric definitions: https://analytics-flights.gonor.me
select
  concat_ws('|', f.departure_airport, f.arrival_airport, date_format(f.scheduled_departure, 'yyyy-MM'), tf.fare_conditions) as _id,
  f.departure_airport as dep_airport,
  f.arrival_airport as arr_airport,
  f.departure_city as dep_city,
  f.arrival_city as arr_city,
  date_format(f.scheduled_departure, 'yyyy-MM') as month,
  tf.fare_conditions as fare_conditions,
  cast(sum(tf.amount) as double) as revenue,
  cast(count(tf.ticket_no) as int) as tickets
from {{ ref('silver_flights_enriched') }} f
join {{ ref('silver_ticket_flights') }} tf on f.flight_id = tf.flight_id
group by
  f.departure_airport,
  f.arrival_airport,
  f.departure_city,
  f.arrival_city,
  date_format(f.scheduled_departure, 'yyyy-MM'),
  tf.fare_conditions
