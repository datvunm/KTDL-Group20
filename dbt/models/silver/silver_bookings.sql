{{ config(
    materialized = 'table',
    schema = 'silver'
) }}

with ranked_source as (
    select
        book_ref,
        book_date,
        cast(total_amount as double) as total_amount,
        row_number() over (partition by book_ref order by _ingest_ts desc) as rn
    from {{ source('bronze', 'bookings') }}
)

select
    book_ref,
    book_date,
    total_amount
from ranked_source
where rn = 1
