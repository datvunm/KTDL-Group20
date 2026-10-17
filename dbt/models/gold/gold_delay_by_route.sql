{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

-- Official demo DB sample queries: https://postgrespro.com/docs/postgrespro/9.6/apjs05
-- Metric definitions: https://analytics-flights.gonor.me
select
  concat_ws('|', departure_airport, arrival_airport) as _id,
  departure_airport as dep_airport,
  arrival_airport as arr_airport,
  departure_city as dep_city,
  arrival_city as arr_city,
  cast(count(*) as int) as flights,
  cast(count(case when dep_delay_min > 15 then 1 end) as int) as delayed,
  cast(count(case when dep_delay_min > 15 then 1 end) / count(*) as double) as delay_rate,
  cast(coalesce(avg(case when dep_delay_min > 15 then dep_delay_min end), 0.0) as double) as avg_delay_min
from {{ ref('silver_flights_enriched') }}
where status = 'Arrived'
group by departure_airport, arrival_airport, departure_city, arrival_city
having count(*) >= 20
