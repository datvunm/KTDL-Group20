{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

-- Official demo DB sample queries: https://postgrespro.com/docs/postgrespro/9.6/apjs05
-- Metric definitions: https://analytics-flights.gonor.me
with base as (
  select
    cast(case when dayofweek(scheduled_departure_local) = 1 then 7 else dayofweek(scheduled_departure_local) - 1 end as int) as dow,
    cast(hour(scheduled_departure_local) as int) as hour,
    case when dep_delay_min > 15 then 1 else 0 end as is_delayed
  from {{ ref('silver_flights_enriched') }}
  where status = 'Arrived'
)
select
  concat_ws('|', cast(dow as string), cast(hour as string)) as _id,
  dow,
  hour,
  cast(count(*) as int) as flights,
  cast(sum(is_delayed) as int) as delayed,
  cast(sum(is_delayed) / count(*) as double) as delay_rate
from base
group by dow, hour
