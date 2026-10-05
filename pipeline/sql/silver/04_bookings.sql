-- Silver Bookings (from raw flight_seat_reservations rows that carry a booking)
CREATE TABLE IF NOT EXISTS lake.silver.bookings (
  book_ref string,
  book_date timestamp,
  total_amount double
) USING iceberg;

-- A booking repeats on every seat/segment row of its tickets; collapse to one
-- row per book_ref. Different book_date / total_amount for one book_ref is conflicting
CREATE OR REPLACE TEMPORARY VIEW bookings_batch AS
SELECT
  *,
  CASE
    WHEN book_date IS NULL THEN 'missing book_date'
    WHEN total_amount < 0 THEN 'total_amount < 0'
    WHEN count(*) OVER (PARTITION BY book_ref) > 1 THEN 'conflicting booking attributes'
  END AS reject_reason
FROM (
  SELECT DISTINCT book_ref, book_date, total_amount
  FROM lake.bronze.flight_seat_reservations
  WHERE _batch_id = '${run_id}'
    AND book_ref IS NOT NULL
);

INSERT INTO lake.silver.quarantine
SELECT
  'flight_seat_reservations' AS source_table,
  reject_reason AS reason,
  to_json(struct(book_ref, book_date, total_amount)) AS payload,
  '${run_id}' AS _batch_id,
  current_timestamp() AS _ingest_ts
FROM bookings_batch
WHERE reject_reason IS NOT NULL;

MERGE INTO lake.silver.bookings AS target
USING (
  SELECT
    book_ref,
    book_date,
    cast(total_amount as double) AS total_amount
  FROM bookings_batch
  WHERE reject_reason IS NULL
) AS source
ON target.book_ref = source.book_ref
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
