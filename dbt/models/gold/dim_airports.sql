{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

-- Official demo DB sample queries: https://postgrespro.com/docs/postgrespro/9.6/apjs05
-- Metric definitions: https://analytics-flights.gonor.me
select
  a.airport_code as _id,
  a.airport_code,
  a.airport_name,
  a.city,
  a.lon,
  a.lat,
  a.timezone,
  cast(count(f.flight_id) as int) as departures
from {{ ref('silver_airports') }} a
left join {{ ref('silver_flights_enriched') }} f on a.airport_code = f.departure_airport
group by a.airport_code, a.airport_name, a.city, a.lon, a.lat, a.timezone
