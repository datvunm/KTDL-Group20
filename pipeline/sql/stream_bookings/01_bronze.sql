CREATE TABLE IF NOT EXISTS lake.bronze.booking_events (
  book_ref string,
  book_date timestamp,
  total_amount double,
  segments array<struct<
    ticket_no:string, passenger_id:string, passenger_name:string,
    flight_id:int, flight_no:string, departure_airport:string,
    arrival_airport:string, aircraft_code:string,
    scheduled_departure:timestamp, scheduled_arrival:timestamp,
    fare_conditions:string, amount:double
  >>,
  _ingest_ts timestamp,
  _batch_id string
) USING iceberg;

-- Replayed on retry of the same micro-batch, so clear this batch's rows first.
DELETE FROM lake.bronze.booking_events WHERE _batch_id = '${batch_id}';

INSERT INTO lake.bronze.booking_events
SELECT * FROM booking_events_batch;
