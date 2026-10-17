{{ config(
    materialized = 'table',
    schema = 'silver'
) }}

with ranked_source as (
    select
        ticket_no,
        book_ref,
        sha2(passenger_id, 256) as passenger_key,
        row_number() over (partition by ticket_no order by _ingest_ts desc) as rn
    from {{ source('bronze', 'tickets') }}
)

select
    ticket_no,
    book_ref,
    passenger_key
from ranked_source
where rn = 1
