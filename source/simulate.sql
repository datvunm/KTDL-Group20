-- source/simulate.sql
-- Deterministic simulation function of cutoff (truncate + reinsert).
-- Usage in demo db: psql -v cutoff="'<cutoff>'" -f simulate.sql

BEGIN;

SET LOCAL work_mem = '256MB';

-- 1. Truncate all 8 bookings tables in reverse FK dependency order
TRUNCATE TABLE
    bookings.boarding_passes,
    bookings.ticket_flights,
    bookings.flights,
    bookings.tickets,
    bookings.bookings,
    bookings.seats,
    bookings.airports_data,
    bookings.aircrafts_data;

-- 2. Full copy of dimension tables
INSERT INTO bookings.aircrafts_data (aircraft_code, model, range)
SELECT aircraft_code, model, range
FROM archive.aircrafts_data;

INSERT INTO bookings.airports_data (airport_code, airport_name, city, coordinates, timezone)
SELECT airport_code, airport_name, city, coordinates, timezone
FROM archive.airports_data;

INSERT INTO bookings.seats (aircraft_code, seat_no, fare_conditions)
SELECT aircraft_code, seat_no, fare_conditions
FROM archive.seats;

-- 3. Bookings released as of cutoff
INSERT INTO bookings.bookings (book_ref, book_date, total_amount)
SELECT book_ref, book_date, total_amount
FROM archive.bookings
WHERE book_date <= CAST(:cutoff AS timestamptz);

-- 4. Tickets belonging to released bookings
INSERT INTO bookings.tickets (ticket_no, book_ref, passenger_id, passenger_name, contact_data)
SELECT t.ticket_no, t.book_ref, t.passenger_id, t.passenger_name, t.contact_data
FROM archive.tickets t
JOIN bookings.bookings b ON b.book_ref = t.book_ref;

-- 5. Collect released flight IDs:
-- Flights scheduled within 31 days of cutoff OR referenced by released ticket_flights.
-- Using temporary table for optimal planner execution on large datasets.
CREATE TEMP TABLE _released_flight_ids (
    flight_id integer PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO _released_flight_ids (flight_id)
SELECT a.flight_id
FROM archive.flights a
WHERE a.scheduled_departure <= CAST(:cutoff AS timestamptz) + interval '31 days'
UNION
SELECT tf.flight_id
FROM archive.ticket_flights tf
JOIN bookings.tickets t ON t.ticket_no = tf.ticket_no;

-- 6. Insert released flights with state computed as of cutoff:
-- Flight state rules (a = archive row):
-- - a.status = 'Cancelled' -> Cancelled, actuals NULL.
-- - actual_departure = a.actual_departure if <= cutoff else NULL.
-- - actual_arrival = a.actual_arrival if <= cutoff (and departure kept) else NULL.
-- - status:
--     Arrived if actual_arrival not null;
--     Departed if actual_departure not null;
--     else if scheduled_departure - interval '24 hours' <= cutoff:
--         Delayed if a.status = 'Delayed' OR a.actual_departure > scheduled_departure
--         else On Time;
--     else Scheduled.
INSERT INTO bookings.flights (
    flight_id,
    flight_no,
    scheduled_departure,
    scheduled_arrival,
    departure_airport,
    arrival_airport,
    status,
    aircraft_code,
    actual_departure,
    actual_arrival
)
SELECT
    a.flight_id,
    a.flight_no,
    a.scheduled_departure,
    a.scheduled_arrival,
    a.departure_airport,
    a.arrival_airport,
    CASE
        WHEN a.status = 'Cancelled' THEN 'Cancelled'
        WHEN (a.actual_departure IS NOT NULL AND a.actual_departure <= CAST(:cutoff AS timestamptz)
              AND a.actual_arrival IS NOT NULL AND a.actual_arrival <= CAST(:cutoff AS timestamptz)) THEN 'Arrived'
        WHEN (a.actual_departure IS NOT NULL AND a.actual_departure <= CAST(:cutoff AS timestamptz)) THEN 'Departed'
        WHEN a.scheduled_departure - interval '24 hours' <= CAST(:cutoff AS timestamptz) THEN
            CASE
                WHEN a.status = 'Delayed' OR (a.actual_departure IS NOT NULL AND a.actual_departure > a.scheduled_departure) THEN 'Delayed'
                ELSE 'On Time'
            END
        ELSE 'Scheduled'
    END AS status,
    a.aircraft_code,
    CASE
        WHEN a.status = 'Cancelled' THEN NULL
        WHEN a.actual_departure IS NOT NULL AND a.actual_departure <= CAST(:cutoff AS timestamptz) THEN a.actual_departure
        ELSE NULL
    END AS actual_departure,
    CASE
        WHEN a.status = 'Cancelled' THEN NULL
        WHEN a.actual_departure IS NOT NULL AND a.actual_departure <= CAST(:cutoff AS timestamptz)
             AND a.actual_arrival IS NOT NULL AND a.actual_arrival <= CAST(:cutoff AS timestamptz) THEN a.actual_arrival
        ELSE NULL
    END AS actual_arrival
FROM archive.flights a
JOIN _released_flight_ids r ON r.flight_id = a.flight_id;

-- 7. Ticket flights of released tickets
INSERT INTO bookings.ticket_flights (ticket_no, flight_id, fare_conditions, amount)
SELECT tf.ticket_no, tf.flight_id, tf.fare_conditions, tf.amount
FROM archive.ticket_flights tf
JOIN bookings.tickets t ON t.ticket_no = tf.ticket_no;

-- 8. Boarding passes: archive passes of released tickets whose flight
-- scheduled_departure - interval '24 hours' <= cutoff and flight not Cancelled
INSERT INTO bookings.boarding_passes (ticket_no, flight_id, boarding_no, seat_no)
SELECT bp.ticket_no, bp.flight_id, bp.boarding_no, bp.seat_no
FROM archive.boarding_passes bp
JOIN bookings.tickets t ON t.ticket_no = bp.ticket_no
JOIN bookings.flights f ON f.flight_id = bp.flight_id
WHERE f.scheduled_departure - interval '24 hours' <= CAST(:cutoff AS timestamptz)
  AND f.status <> 'Cancelled';

-- 9. Update simulation state
UPDATE bookings.sim_state SET cutoff = CAST(:cutoff AS timestamptz);
INSERT INTO bookings.sim_state (cutoff)
SELECT CAST(:cutoff AS timestamptz)
WHERE NOT EXISTS (SELECT 1 FROM bookings.sim_state);

COMMIT;
