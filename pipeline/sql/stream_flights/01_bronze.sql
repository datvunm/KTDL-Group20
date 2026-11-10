CREATE TABLE IF NOT EXISTS lake.bronze.flight_events (
  flight_id int, flight_no string, scheduled_departure timestamp, scheduled_arrival timestamp,
  departure_airport string, arrival_airport string, aircraft_code string, status string,
  actual_departure timestamp, actual_arrival timestamp,
  _ingest_ts timestamp, _batch_id string
) USING iceberg;

DELETE FROM lake.bronze.flight_events WHERE _batch_id = '${batch_id}';

INSERT INTO lake.bronze.flight_events
SELECT * FROM flight_events_batch;
