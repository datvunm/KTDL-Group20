-- Silver Tickets
CREATE TABLE IF NOT EXISTS lake.silver.tickets (
  ticket_no string,
  book_ref string,
  passenger_key string
) USING iceberg;

MERGE INTO lake.silver.tickets AS target
USING (
  SELECT
    ticket_no,
    book_ref,
    passenger_key
  FROM (
    SELECT
      ticket_no,
      book_ref,
      sha2(passenger_id, 256) AS passenger_key,
      row_number() OVER (PARTITION BY ticket_no ORDER BY _ingest_ts DESC) AS rn
    FROM lake.bronze.tickets
    WHERE _batch_id = '${run_id}'
  )
  WHERE rn = 1
) AS source
ON target.ticket_no = source.ticket_no
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
