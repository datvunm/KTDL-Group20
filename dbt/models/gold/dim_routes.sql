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
  departure_lon as dep_lon,
  departure_lat as dep_lat,
  arrival_lon as arr_lon,
  arrival_lat as arr_lat,
  cast(count(*) as int) as flights
from {{ ref('silver_flights_enriched') }}
group by
  departure_airport,
  arrival_airport,
  departure_lon,
  departure_lat,
  arrival_lon,
  arrival_lat
order by count(*) desc
limit 100
