-- Silver Tickets (from raw flight_seat_reservations rows that carry a ticket)
CREATE TABLE IF NOT EXISTS lake.silver.tickets (
  ticket_no string,
  book_ref string,
  passenger_key string
) USING iceberg;

-- A ticket repeats on each of its flight segments; collapse to one row per
-- ticket_no. passenger_name / contact_data stay in bronze (PII is not curated)
CREATE OR REPLACE TEMPORARY VIEW tickets_batch AS
SELECT
  *,
  CASE
    WHEN book_ref IS NULL THEN 'missing book_ref'
    WHEN passenger_id IS NULL THEN 'missing passenger_id'
    WHEN count(*) OVER (PARTITION BY ticket_no) > 1 THEN 'conflicting ticket attributes'
  END AS reject_reason
FROM (
  SELECT DISTINCT ticket_no, book_ref, passenger_id
  FROM lake.bronze.flight_seat_reservations
  WHERE _batch_id = '${run_id}'
    AND ticket_no IS NOT NULL
);

-- Payload hashes passenger_id so quarantine never stores plain PII
INSERT INTO lake.silver.quarantine
SELECT
  'flight_seat_reservations' AS source_table,
  reject_reason AS reason,
  to_json(struct(ticket_no, book_ref, sha2(passenger_id, 256) AS passenger_key)) AS payload,
  '${run_id}' AS _batch_id,
  current_timestamp() AS _ingest_ts
FROM tickets_batch
WHERE reject_reason IS NOT NULL;

MERGE INTO lake.silver.tickets AS target
USING (
  SELECT
    ticket_no,
    book_ref,
    sha2(passenger_id, 256) AS passenger_key
  FROM tickets_batch
  WHERE reject_reason IS NULL
) AS source
ON target.ticket_no = source.ticket_no
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
