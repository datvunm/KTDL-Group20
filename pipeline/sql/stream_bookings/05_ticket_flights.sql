CREATE TABLE IF NOT EXISTS lake.silver.ticket_flights (
  ticket_no string, flight_id int, fare_conditions string, amount double
) USING iceberg PARTITIONED BY (bucket(8, flight_id));

-- Runs after 02_flights.sql and 03_tickets.sql, so both already exist.
MERGE INTO lake.silver.ticket_flights AS target
USING (
  SELECT ticket_no, cast(flight_id as int) AS flight_id, fare_conditions, amount
  FROM segments_batch WHERE ticket_no IS NOT NULL AND flight_id IS NOT NULL
) AS source
ON target.ticket_no = source.ticket_no AND target.flight_id = source.flight_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
