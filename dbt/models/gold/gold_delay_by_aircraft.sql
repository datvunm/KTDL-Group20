{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

-- Official demo DB sample queries: https://postgrespro.com/docs/postgrespro/9.6/apjs05
-- Metric definitions: https://analytics-flights.gonor.me
select
  f.aircraft_code as _id,
  f.aircraft_code,
  coalesce(f.model, ac.model) as model,
  cast(count(*) as int) as flights,
  cast(count(case when f.dep_delay_min > 15 then 1 end) as int) as delayed,
  cast(count(case when f.dep_delay_min > 15 then 1 end) / count(*) as double) as delay_rate,
  cast(coalesce(avg(case when f.dep_delay_min > 15 then f.dep_delay_min end), 0.0) as double) as avg_delay_min
from {{ ref('silver_flights_enriched') }} f
left join {{ ref('silver_aircrafts') }} ac on f.aircraft_code = ac.aircraft_code
where f.status = 'Arrived'
group by f.aircraft_code, coalesce(f.model, ac.model)
