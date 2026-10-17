{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

-- Official demo DB sample queries: https://postgrespro.com/docs/postgrespro/9.6/apjs05
-- Metric definitions: https://analytics-flights.gonor.me
with seat_counts as (
  select
    aircraft_code,
    cast(count(*) as int) as seats_total,
    cast(count(case when fare_conditions = 'Economy' then 1 end) as int) as seats_economy,
    cast(count(case when fare_conditions = 'Comfort' then 1 end) as int) as seats_comfort,
    cast(count(case when fare_conditions = 'Business' then 1 end) as int) as seats_business
  from {{ ref('silver_seats') }}
  group by aircraft_code
),
flight_metrics as (
  select
    fo.aircraft_code,
    cast(count(fo.flight_id) as int) as flights,
    cast(coalesce(sum(f.actual_duration_min) / 60.0, 0.0) as double) as flight_hours,
    cast(coalesce(avg(f.actual_duration_min), 0.0) as double) as avg_duration_min,
    cast(coalesce(avg(fo.load_factor), 0.0) as double) as avg_load_factor
  from {{ ref('gold_flight_occupancy') }} fo
  join {{ ref('silver_flights_enriched') }} f on fo.flight_id = f.flight_id
  group by fo.aircraft_code
)
select
  a.aircraft_code as _id,
  a.aircraft_code,
  a.model,
  cast(a.range as int) as range,
  coalesce(sc.seats_total, 0) as seats_total,
  coalesce(sc.seats_economy, 0) as seats_economy,
  coalesce(sc.seats_comfort, 0) as seats_comfort,
  coalesce(sc.seats_business, 0) as seats_business,
  coalesce(fm.flights, 0) as flights,
  coalesce(fm.flight_hours, 0.0) as flight_hours,
  coalesce(fm.avg_duration_min, 0.0) as avg_duration_min,
  coalesce(fm.avg_load_factor, 0.0) as avg_load_factor
from {{ ref('silver_aircrafts') }} a
left join seat_counts sc on a.aircraft_code = sc.aircraft_code
left join flight_metrics fm on a.aircraft_code = fm.aircraft_code
