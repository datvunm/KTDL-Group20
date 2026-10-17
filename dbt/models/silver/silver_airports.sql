{{ config(
    materialized = 'table',
    schema = 'silver'
) }}

with ranked_source as (
    select
        airport_code,
        get_json_object(airport_name, '$.en') as airport_name,
        get_json_object(city, '$.en') as city,
        cast(regexp_extract(coordinates, '^\\(([^,]+),\\s*([^)]+)\\)$', 1) as double) as lon,
        cast(regexp_extract(coordinates, '^\\(([^,]+),\\s*([^)]+)\\)$', 2) as double) as lat,
        timezone,
        row_number() over (partition by airport_code order by _ingest_ts desc) as rn
    from {{ source('bronze', 'airports_data') }}
)

select
    airport_code,
    airport_name,
    city,
    lon,
    lat,
    timezone
from ranked_source
where rn = 1
