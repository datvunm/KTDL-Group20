-- Flatten each booking's segments array into one row per ticket/flight.
CREATE OR REPLACE TEMPORARY VIEW segments_batch AS
SELECT
  book_ref,
  seg.ticket_no, seg.passenger_id, seg.passenger_name,
  seg.flight_id, seg.flight_no, seg.departure_airport, seg.arrival_airport,
  seg.aircraft_code, seg.scheduled_departure, seg.scheduled_arrival,
  seg.fare_conditions, seg.amount
FROM (
  SELECT book_ref, explode(segments) AS seg FROM booking_events_batch
);
