-- Silver Bookings
CREATE TABLE IF NOT EXISTS lake.silver.bookings (
  book_ref string,
  book_date timestamp,
  total_amount double
) USING iceberg;

MERGE INTO lake.silver.bookings AS target
USING (
  SELECT
    book_ref,
    book_date,
    total_amount
  FROM (
    SELECT
      book_ref,
      book_date,
      cast(total_amount as double) AS total_amount,
      row_number() OVER (PARTITION BY book_ref ORDER BY _ingest_ts DESC) AS rn
    FROM lake.bronze.bookings
    WHERE _batch_id = '${run_id}'
  )
  WHERE rn = 1
) AS source
ON target.book_ref = source.book_ref
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
