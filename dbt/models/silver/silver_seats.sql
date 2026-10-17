{{ config(
    materialized = 'table',
    schema = 'silver'
) }}

with ranked_source as (
    select
        aircraft_code,
        seat_no,
        fare_conditions,
        row_number() over (partition by aircraft_code, seat_no order by _ingest_ts desc) as rn
    from {{ source('bronze', 'seats') }}
)

select
    aircraft_code,
    seat_no,
    fare_conditions
from ranked_source
where rn = 1
