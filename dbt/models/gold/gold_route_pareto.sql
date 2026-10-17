{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

-- Official demo DB sample queries: https://postgrespro.com/docs/postgrespro/9.6/apjs05
-- Metric definitions: https://analytics-flights.gonor.me
with route_rev as (
  select
    f.departure_airport as dep_airport,
    f.arrival_airport as arr_airport,
    f.departure_city as dep_city,
    f.arrival_city as arr_city,
    cast(sum(tf.amount) as double) as revenue
  from {{ ref('silver_flights_enriched') }} f
  join {{ ref('silver_ticket_flights') }} tf on f.flight_id = tf.flight_id
  group by f.departure_airport, f.arrival_airport, f.departure_city, f.arrival_city
),
ranked as (
  select
    dep_airport,
    arr_airport,
    dep_city,
    arr_city,
    revenue,
    cast(row_number() over (order by revenue desc, dep_airport, arr_airport) as int) as rank,
    sum(revenue) over () as total_rev,
    sum(revenue) over (order by revenue desc, dep_airport, arr_airport rows between unbounded preceding and current row) as running_rev
  from route_rev
)
select
  concat_ws('|', dep_airport, arr_airport) as _id,
  dep_airport,
  arr_airport,
  dep_city,
  arr_city,
  revenue,
  rank,
  cast(case when total_rev > 0 then running_rev / total_rev else 0.0 end as double) as cum_share,
  cast(case when total_rev > 0 then revenue / total_rev else 0.0 end as double) as revenue_share
from ranked
