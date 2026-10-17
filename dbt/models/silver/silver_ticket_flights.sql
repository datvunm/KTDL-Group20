{{ config(
    materialized = 'table',
    schema = 'silver'
) }}

with deduped_segments as (
    select *,
        row_number() over (partition by ticket_no, flight_id order by _ingest_ts desc) as rn
    from {{ source('bronze', 'ticket_flights') }}
)

select
    tf.ticket_no,
    tf.flight_id,
    tf.fare_conditions,
    cast(tf.amount as double) as amount
from deduped_segments tf
join {{ ref('silver_flights_enriched') }} f on tf.flight_id = f.flight_id
join {{ ref('silver_tickets') }} t on tf.ticket_no = t.ticket_no
where tf.rn = 1
  and tf.amount >= 0
