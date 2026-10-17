{{ config(
    materialized = 'table',
    schema = 'silver'
) }}

with ranked_source as (
    select
        aircraft_code,
        get_json_object(model, '$.en') as model,
        cast(range as int) as range,
        row_number() over (partition by aircraft_code order by _ingest_ts desc) as rn
    from {{ source('bronze', 'aircrafts_data') }}
)

select
    aircraft_code,
    model,
    range
from ranked_source
where rn = 1
