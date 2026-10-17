-- A flight's departed and arrived events can land in the same micro-batch;
-- MERGE needs exactly one source row per flight_id, so keep the most
-- complete one (arrived over departed).
CREATE OR REPLACE TEMPORARY VIEW flight_events_dedup AS
SELECT flight_id, flight_no, scheduled_departure, scheduled_arrival,
       departure_airport, arrival_airport, aircraft_code, status, actual_departure, actual_arrival
FROM (
  SELECT *, row_number() OVER (
    PARTITION BY flight_id
    ORDER BY actual_arrival IS NOT NULL DESC, actual_departure DESC
  ) AS rn
  FROM flight_events_batch
) WHERE rn = 1;
