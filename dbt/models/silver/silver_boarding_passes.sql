{{ config(
    materialized = 'table',
    schema = 'silver'
) }}

with deduped_passes as (
    select *,
        row_number() over (partition by ticket_no, flight_id order by _ingest_ts desc) as rn
    from {{ source('bronze', 'boarding_passes') }}
)

select
    bp.ticket_no,
    bp.flight_id,
    cast(bp.boarding_no as int) as boarding_no,
    bp.seat_no
from deduped_passes bp
join {{ ref('silver_flights_enriched') }} f on bp.flight_id = f.flight_id
join {{ ref('silver_seats') }} s on f.aircraft_code = s.aircraft_code and bp.seat_no = s.seat_no
where bp.rn = 1
