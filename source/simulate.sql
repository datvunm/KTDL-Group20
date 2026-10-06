-- source/simulate.sql
-- Deterministic simulation function of cutoff (truncate + reinsert).
-- Rebuilds raw.* (state as of cutoff) from archive.* (full history).
-- Usage in demo db: psql -v cutoff="'<cutoff>'" -f simulate.sql

BEGIN;

SET LOCAL work_mem = '256MB';
-- Deterministic regardless of client: a bare :cutoff date and the
-- days_of_week day numbers are read in UTC, as when archive was built.
SET LOCAL TimeZone = 'UTC';

-- 1. Truncate the 3 raw tables
TRUNCATE TABLE
    raw.flight_seat_reservations,
    raw.airport_sites,
    raw.aircraft_seat_layouts;

-- 2. Full copy of reference tables
INSERT INTO raw.airport_sites (airport_code, airport_name, city, coordinates, timezone)
SELECT airport_code, airport_name, city, coordinates, timezone
FROM archive.airport_sites;

INSERT INTO raw.aircraft_seat_layouts (aircraft_code, model, range, seat_no, seats_fare_conditions)
SELECT aircraft_code, model, range, seat_no, seats_fare_conditions
FROM archive.aircraft_seat_layouts;

-- 3. Released flights with state computed as of cutoff.
-- A flight is released if scheduled within 31 days of cutoff OR booked by a
-- released booking (book_date <= cutoff), e.g. advance bookings > 30 days.
-- Flight state rules (a = archive flight):
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
-- checkin_open: boarding passes are visible once check-in has opened
-- (scheduled_departure - interval '24 hours' <= cutoff) and flight not Cancelled.
CREATE TEMP TABLE _flights ON COMMIT DROP AS
SELECT
    a.flight_id,
    a.flight_no,
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
    END::varchar(20) AS status,
    a.scheduled_departure,
    a.scheduled_arrival,
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
    END AS actual_arrival,
    a.departure_airport,
    a.arrival_airport,
    a.aircraft_code,
    (a.status <> 'Cancelled'
     AND a.scheduled_departure - interval '24 hours' <= CAST(:cutoff AS timestamptz)) AS checkin_open
FROM (
    SELECT flight_id, flight_no, status, scheduled_departure, scheduled_arrival,
           actual_departure, actual_arrival, departure_airport, arrival_airport, aircraft_code,
           bool_or(book_date <= CAST(:cutoff AS timestamptz)) AS has_released_booking
    FROM archive.flight_seat_reservations
    GROUP BY flight_id, flight_no, status, scheduled_departure, scheduled_arrival,
             actual_departure, actual_arrival, departure_airport, arrival_airport, aircraft_code
) a
WHERE a.scheduled_departure <= CAST(:cutoff AS timestamptz) + interval '31 days'
   OR a.has_released_booking;

ALTER TABLE _flights ADD PRIMARY KEY (flight_id);

-- 4. Route timetable per flight number over released flights
-- (same definition as the dump's bookings.routes view)
CREATE TEMP TABLE _routes ON COMMIT DROP AS
SELECT f.flight_no,
       f.departure_airport,
       f.arrival_airport,
       f.aircraft_code,
       f.scheduled_arrival - f.scheduled_departure AS duration,
       array_agg(DISTINCT to_char(f.scheduled_departure, 'ID')::integer
                 ORDER BY to_char(f.scheduled_departure, 'ID')::integer) AS days_of_week
FROM _flights f
GROUP BY f.flight_no, f.departure_airport, f.arrival_airport,
         f.aircraft_code, f.scheduled_arrival - f.scheduled_departure;

-- 5. Seat and booking rows of released flights.
-- Each archive row yields at most two rows (v.seat_row):
--   seat_row = true  : the physical seat; the passenger stays on it only if
--                      the booking is released and check-in has opened,
--                      otherwise the seat is empty.
--   seat_row = false : a released booking without a seat yet: either it has
--                      no seat in the archive, or check-in has not opened.
-- Tickets of bookings after cutoff disappear.
INSERT INTO raw.flight_seat_reservations (
    flight_id, flight_no, status, scheduled_departure, scheduled_arrival,
    actual_departure, actual_arrival, departure_airport, arrival_airport, aircraft_code,
    days_of_week, duration,
    seat_no, boarding_no,
    ticket_no, ticket_flights_fare_conditions, amount,
    book_ref, passenger_id, passenger_name, contact_data,
    book_date, total_amount
)
SELECT
    f.flight_id, f.flight_no, f.status, f.scheduled_departure, f.scheduled_arrival,
    f.actual_departure, f.actual_arrival, f.departure_airport, f.arrival_airport, f.aircraft_code,
    r.days_of_week, r.duration,
    CASE WHEN v.seat_row THEN x.seat_no END,
    CASE WHEN v.seat_row AND x.booked AND f.checkin_open THEN x.boarding_no END,
    CASE WHEN x.keep THEN x.ticket_no END,
    CASE WHEN x.keep THEN x.ticket_flights_fare_conditions END,
    CASE WHEN x.keep THEN x.amount END,
    CASE WHEN x.keep THEN x.book_ref END,
    CASE WHEN x.keep THEN x.passenger_id END,
    CASE WHEN x.keep THEN x.passenger_name END,
    CASE WHEN x.keep THEN x.contact_data END,
    CASE WHEN x.keep THEN x.book_date END,
    CASE WHEN x.keep THEN x.total_amount END
FROM archive.flight_seat_reservations a
JOIN _flights f ON f.flight_id = a.flight_id
JOIN _routes r ON r.flight_no         = f.flight_no
              AND r.departure_airport = f.departure_airport
              AND r.arrival_airport   = f.arrival_airport
              AND r.aircraft_code     = f.aircraft_code
              AND r.duration          = f.scheduled_arrival - f.scheduled_departure
CROSS JOIN LATERAL (
    SELECT a.ticket_no IS NOT NULL AND a.book_date <= CAST(:cutoff AS timestamptz) AS booked
) b
CROSS JOIN LATERAL (VALUES (true), (false)) v(seat_row)
CROSS JOIN LATERAL (
    SELECT a.seat_no, a.boarding_no, a.ticket_no, a.ticket_flights_fare_conditions, a.amount,
           a.book_ref, a.passenger_id, a.passenger_name, a.contact_data, a.book_date, a.total_amount,
           b.booked,
           -- passenger columns are kept on the booking row, and on the seat row once checked in
           b.booked AND (NOT v.seat_row OR f.checkin_open) AS keep
) x
WHERE (v.seat_row AND a.seat_no IS NOT NULL)
   OR (NOT v.seat_row AND b.booked AND (a.seat_no IS NULL OR NOT f.checkin_open));

-- 6. Update simulation state
UPDATE raw.sim_state SET cutoff = CAST(:cutoff AS timestamptz);
INSERT INTO raw.sim_state (cutoff)
SELECT CAST(:cutoff AS timestamptz)
WHERE NOT EXISTS (SELECT 1 FROM raw.sim_state);

COMMIT;
