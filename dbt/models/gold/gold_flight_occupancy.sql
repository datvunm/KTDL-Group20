{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

-- Official demo DB sample queries: https://postgrespro.com/docs/postgrespro/9.6/apjs05
-- Metric definitions: https://analytics-flights.gonor.me
with plane_seats as (
  select aircraft_code, cast(count(*) as int) as seats
  from {{ ref('silver_seats') }}
  group by aircraft_code
),
flight_boarded as (
  select flight_id, cast(count(*) as int) as boarded
  from {{ ref('silver_boarding_passes') }}
  group by flight_id
)
select
  cast(f.flight_id as string) as _id,
  cast(f.flight_id as int) as flight_id,
  f.flight_no,
  f.departure_airport as dep_airport,
  f.arrival_airport as arr_airport,
  f.aircraft_code,
  f.model,
  date_format(f.scheduled_departure, 'yyyy-MM') as month,
  date_format(f.scheduled_departure, "yyyy-MM-dd'T'HH:mm:ss'Z'") as scheduled_departure,
  cast(coalesce(fb.boarded, 0) as int) as boarded,
  cast(coalesce(ps.seats, 0) as int) as seats,
  cast(case when coalesce(ps.seats, 0) > 0 then coalesce(fb.boarded, 0) / ps.seats else 0.0 end as double) as load_factor
from {{ ref('silver_flights_enriched') }} f
left join plane_seats ps on f.aircraft_code = ps.aircraft_code
left join flight_boarded fb on f.flight_id = fb.flight_id
where f.status in ('Departed', 'Arrived')
