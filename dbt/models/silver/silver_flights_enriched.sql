{{ config(
    materialized = 'table',
    schema = 'silver'
) }}

with deduped_flights as (
    select *,
        row_number() over (partition by flight_id order by _ingest_ts desc) as rn
    from {{ source('bronze', 'flights') }}
),

valid_flights as (
    select *
    from deduped_flights
    where rn = 1
      and not (
        (actual_arrival is not null and actual_departure is not null and actual_arrival <= actual_departure)
        or (status not in ('On Time', 'Delayed', 'Departed', 'Arrived', 'Scheduled', 'Cancelled'))
      )
)

select
    f.flight_id,
    f.flight_no,
    f.scheduled_departure,
    f.scheduled_arrival,
    f.departure_airport,
    f.arrival_airport,
    f.status,
    f.aircraft_code,
    f.actual_departure,
    f.actual_arrival,
    dep.airport_name as departure_airport_name,
    dep.city as departure_city,
    dep.lon as departure_lon,
    dep.lat as departure_lat,
    dep.timezone as departure_timezone,
    arr.airport_name as arrival_airport_name,
    arr.city as arrival_city,
    arr.lon as arrival_lon,
    arr.lat as arrival_lat,
    arr.timezone as arrival_timezone,
    ac.model as model,
    from_utc_timestamp(f.scheduled_departure, coalesce(dep.timezone, 'UTC')) as scheduled_departure_local,
    case
      when f.actual_departure is not null
      then from_utc_timestamp(f.actual_departure, coalesce(dep.timezone, 'UTC'))
      else null
    end as actual_departure_local,
    round(2 * 6371 * asin(sqrt(
      pow(sin(radians(arr.lat - dep.lat) / 2), 2) +
      cos(radians(dep.lat)) * cos(radians(arr.lat)) *
      pow(sin(radians(arr.lon - dep.lon) / 2), 2)
    )), 2) as distance_km,
    cast((unix_timestamp(f.scheduled_arrival) - unix_timestamp(f.scheduled_departure)) / 60.0 as double) as scheduled_duration_min,
    case
      when f.actual_arrival is not null and f.actual_departure is not null
      then cast((unix_timestamp(f.actual_arrival) - unix_timestamp(f.actual_departure)) / 60.0 as double)
      else null
    end as actual_duration_min,
    case
      when f.actual_departure is not null
      then cast((unix_timestamp(f.actual_departure) - unix_timestamp(f.scheduled_departure)) / 60.0 as double)
      else null
    end as dep_delay_min
from valid_flights f
left join {{ ref('silver_airports') }} dep on f.departure_airport = dep.airport_code
left join {{ ref('silver_airports') }} arr on f.arrival_airport = arr.airport_code
left join {{ ref('silver_aircrafts') }} ac on f.aircraft_code = ac.aircraft_code
