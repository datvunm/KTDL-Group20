CREATE TABLE IF NOT EXISTS lake.silver.tickets (
  ticket_no string, book_ref string, passenger_key string
) USING iceberg;

MERGE INTO lake.silver.tickets AS target
USING (
  SELECT DISTINCT ticket_no, book_ref, sha2(passenger_id, 256) AS passenger_key
  FROM segments_batch WHERE ticket_no IS NOT NULL
) AS source
ON target.ticket_no = source.ticket_no
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
