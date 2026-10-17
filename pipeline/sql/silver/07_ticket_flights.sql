-- Silver Ticket Flights (from raw flight_seat_reservations rows that carry a ticket)
CREATE TABLE IF NOT EXISTS lake.silver.ticket_flights (
  ticket_no string,
  flight_id int,
  fare_conditions string,
  amount double
) USING iceberg
PARTITIONED BY (bucket(8, flight_id));

-- Each booked segment appears once (seated or not); collapse defensively to one
-- row per (ticket_no, flight_id) and check amount and references
CREATE OR REPLACE TEMPORARY VIEW ticket_flights_batch AS
SELECT
  tf.*,
  CASE
    WHEN tf.n_versions > 1 THEN 'conflicting segment attributes'
    WHEN tf.amount < 0 THEN 'amount < 0'
    WHEN tf.fare_conditions IS NULL OR tf.fare_conditions NOT IN ('Economy', 'Comfort', 'Business')
      THEN 'invalid fare_conditions'
    WHEN f.flight_id IS NULL THEN 'flight_id not in silver flights'
    WHEN t.ticket_no IS NULL THEN 'ticket_no not in silver tickets'
  END AS reject_reason
FROM (
  SELECT *, count(*) OVER (PARTITION BY ticket_no, flight_id) AS n_versions
  FROM (
    SELECT DISTINCT
      ticket_no,
      flight_id,
      ticket_flights_fare_conditions AS fare_conditions,
      amount
    FROM lake.bronze.flight_seat_reservations
    WHERE _batch_id = '${run_id}'
      AND ticket_no IS NOT NULL
  )
) tf
LEFT JOIN lake.silver.flights_enriched f ON tf.flight_id = f.flight_id
LEFT JOIN lake.silver.tickets t ON tf.ticket_no = t.ticket_no;

INSERT INTO lake.silver.quarantine
SELECT
  'flight_seat_reservations' AS source_table,
  reject_reason AS reason,
  to_json(struct(ticket_no, flight_id, fare_conditions, amount)) AS payload,
  '${run_id}' AS _batch_id,
  current_timestamp() AS _ingest_ts
FROM ticket_flights_batch
WHERE reject_reason IS NOT NULL;

MERGE INTO lake.silver.ticket_flights AS target
USING (
  SELECT
    ticket_no,
    cast(flight_id as int) AS flight_id,
    fare_conditions,
    cast(amount as double) AS amount
  FROM ticket_flights_batch
  WHERE reject_reason IS NULL
) AS source
ON target.ticket_no = source.ticket_no AND target.flight_id = source.flight_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
