{{ config(
    materialized = 'table',
    schema = 'silver'
) }}

with bad_flights as (
    select
        'flights' as source_table,
        case
            when actual_arrival is not null and actual_departure is not null and actual_arrival <= actual_departure
                then 'actual_arrival <= actual_departure'
            when status not in ('On Time', 'Delayed', 'Departed', 'Arrived', 'Scheduled', 'Cancelled')
                then 'invalid_status'
            else 'unknown'
        end as reason,
        to_json(struct(
            flight_id, flight_no, scheduled_departure, scheduled_arrival,
            departure_airport, arrival_airport, status, aircraft_code,
            actual_departure, actual_arrival
        )) as payload,
        _batch_id,
        _ingest_ts
    from {{ source('bronze', 'flights') }}
    where (actual_arrival is not null and actual_departure is not null and actual_arrival <= actual_departure)
       or (status not in ('On Time', 'Delayed', 'Departed', 'Arrived', 'Scheduled', 'Cancelled'))
),

bad_ticket_flights as (
    select
        'ticket_flights' as source_table,
        case
            when tf.amount < 0 then 'amount < 0'
            when f.flight_id is null then 'flight_id not in silver flights'
            when t.ticket_no is null then 'ticket_no not in silver tickets'
            else 'unknown'
        end as reason,
        to_json(struct(
            tf.ticket_no, tf.flight_id, tf.fare_conditions, tf.amount
        )) as payload,
        tf._batch_id,
        tf._ingest_ts
    from {{ source('bronze', 'ticket_flights') }} tf
    left join {{ ref('silver_flights_enriched') }} f on tf.flight_id = f.flight_id
    left join {{ ref('silver_tickets') }} t on tf.ticket_no = t.ticket_no
    where tf.amount < 0
       or f.flight_id is null
       or t.ticket_no is null
)

select * from bad_flights
union all
select * from bad_ticket_flights
