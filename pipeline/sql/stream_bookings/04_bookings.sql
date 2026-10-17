CREATE TABLE IF NOT EXISTS lake.silver.bookings (
  book_ref string, book_date timestamp, total_amount double
) USING iceberg;

MERGE INTO lake.silver.bookings AS target
USING (SELECT DISTINCT book_ref, book_date, total_amount FROM booking_events_batch) AS source
ON target.book_ref = source.book_ref
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
