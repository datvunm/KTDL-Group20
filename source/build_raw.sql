-- =====================================================================
-- Raw data source for the big data project (airline demo database,
-- PostgresPro demo-medium-en-20170815)
--
--   1. flight_seat_reservations : customer + flight activity (transactional)
--   2. airport_sites            : airport sites (reference)
--   3. aircraft_seat_layouts    : aircraft models + seat map (reference)
--
-- Principles:
--   * Values are copied as-is from the source tables (no derived columns,
--     no type conversion). point / array / interval / jsonb types are kept
--     and export as text.
--   * Original column names; a table prefix is added only where two
--     source tables share a column name.
--   * Reference data is stored once, in the reference tables. The
--     booking table carries only the keys.
--
-- Source tables (schema bookings, resolved through search_path):
--   aircrafts_data, airports_data, seats, flights, bookings, tickets,
--   ticket_flights, boarding_passes.
--   The 2017 dump has no routes table (only a view, which is not copied
--   to the simulated bookings schema), so the route timetable is derived
--   from flights with the same logic as the dump's bookings.routes view.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. flight_seat_reservations
--
-- Grain: one row per seat per flight, plus one row per booked passenger
--        who has not checked in yet (no seat assigned).
--   * Empty seat           -> ticket / passenger / booking columns are NULL
--   * Booked, no check-in  -> seat_no and boarding columns are NULL
--
-- Keys to the reference tables:
--   departure_airport, arrival_airport -> airport_sites.airport_code
--   aircraft_code + seat_no            -> aircraft_seat_layouts
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS flight_seat_reservations;

CREATE TABLE flight_seat_reservations AS
WITH routes AS (
    -- timetable per flight number (same definition as bookings.routes view)
    SELECT f.flight_no,
           f.departure_airport,
           f.arrival_airport,
           f.aircraft_code,
           f.scheduled_arrival - f.scheduled_departure AS duration,
           array_agg(DISTINCT to_char(f.scheduled_departure, 'ID')::integer
                     ORDER BY to_char(f.scheduled_departure, 'ID')::integer) AS days_of_week
    FROM flights f
    GROUP BY f.flight_no, f.departure_airport, f.arrival_airport,
             f.aircraft_code, f.scheduled_arrival - f.scheduled_departure
),
slots AS (
    -- every physical seat on every flight, with the ticket if someone sits there
    SELECT f.flight_id,
           se.seat_no,
           bp.ticket_no
    FROM flights f
    JOIN seats se ON se.aircraft_code = f.aircraft_code
    LEFT JOIN boarding_passes bp ON bp.flight_id = f.flight_id
                                AND bp.seat_no   = se.seat_no

    UNION ALL

    -- booked passengers who have no seat yet (not checked in)
    SELECT tf.flight_id,
           NULL AS seat_no,
           tf.ticket_no
    FROM ticket_flights tf
    WHERE NOT EXISTS (
        SELECT 1
        FROM boarding_passes bp
        WHERE bp.ticket_no = tf.ticket_no
          AND bp.flight_id = tf.flight_id
    )
)
SELECT
    -- flights
    f.flight_id,
    f.flight_no,
    f.status,
    f.scheduled_departure,
    f.scheduled_arrival,
    f.actual_departure,
    f.actual_arrival,
    f.departure_airport,
    f.arrival_airport,
    f.aircraft_code,

    -- routes (timetable the flight number belongs to)
    r.days_of_week,
    r.duration,

    -- seat (key only; seat details live in aircraft_seat_layouts)
    x.seat_no,

    -- boarding_passes
    bp.boarding_no,

    -- ticket_flights
    tf.ticket_no,
    tf.fare_conditions AS ticket_flights_fare_conditions,
    tf.amount,

    -- tickets
    t.book_ref,
    t.passenger_id,
    t.passenger_name,
    t.contact_data,

    -- bookings
    b.book_date,
    b.total_amount

FROM slots x
JOIN flights f ON f.flight_id = x.flight_id
JOIN routes  r ON r.flight_no         = f.flight_no
              AND r.departure_airport = f.departure_airport
              AND r.arrival_airport   = f.arrival_airport
              AND r.aircraft_code     = f.aircraft_code
              AND r.duration          = f.scheduled_arrival - f.scheduled_departure
LEFT JOIN boarding_passes bp ON bp.flight_id = x.flight_id
                            AND bp.ticket_no = x.ticket_no
LEFT JOIN ticket_flights tf ON tf.ticket_no = x.ticket_no
                           AND tf.flight_id = x.flight_id
LEFT JOIN tickets  t ON t.ticket_no = tf.ticket_no
LEFT JOIN bookings b ON b.book_ref  = t.book_ref;


-- ---------------------------------------------------------------------
-- 2. airport_sites
--
-- Grain: one row per airport (all airports, used by a flight or not).
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS airport_sites;

CREATE TABLE airport_sites AS
SELECT
    a.airport_code,
    a.airport_name,
    a.city,
    a.coordinates,
    a.timezone
FROM airports_data a;


-- ---------------------------------------------------------------------
-- 3. aircraft_seat_layouts
--
-- Grain: one row per seat per aircraft model (the seat map).
-- Aircraft without any seats are kept, with seat columns NULL.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS aircraft_seat_layouts;

CREATE TABLE aircraft_seat_layouts AS
SELECT
    -- aircrafts_data
    ac.aircraft_code,
    ac.model,
    ac.range,

    -- seats
    se.seat_no,
    se.fare_conditions AS seats_fare_conditions
FROM aircrafts_data ac
LEFT JOIN seats se ON se.aircraft_code = ac.aircraft_code;


-- =====================================================================
-- Validation
-- =====================================================================

-- Every booked flight segment must appear exactly once in flight_seat_reservations.
-- ticket_flights and booked_rows must be equal.
SELECT
    (SELECT count(*) FROM ticket_flights)                                        AS ticket_flights,
    (SELECT count(*) FROM flight_seat_reservations WHERE ticket_no IS NOT NULL)  AS booked_rows,
    (SELECT count(*) FROM flight_seat_reservations WHERE ticket_no IS NULL)      AS empty_seat_rows;

-- Reference tables must be lossless copies of their sources.
SELECT
    (SELECT count(*) FROM airports_data) AS airports_src,
    (SELECT count(*) FROM airport_sites)  AS airports_raw,
    (SELECT count(*) FROM seats)         AS seats_src,
    (SELECT count(*) FROM aircraft_seat_layouts WHERE seat_no IS NOT NULL) AS seats_raw;

-- Every key in the booking table must resolve in the reference tables.
-- All three counts should be 0.
SELECT
    (SELECT count(*) FROM flight_seat_reservations b
      WHERE NOT EXISTS (SELECT 1 FROM airport_sites a
                        WHERE a.airport_code = b.departure_airport)) AS missing_dep_airport,
    (SELECT count(*) FROM flight_seat_reservations b
      WHERE NOT EXISTS (SELECT 1 FROM airport_sites a
                        WHERE a.airport_code = b.arrival_airport))   AS missing_arr_airport,
    (SELECT count(*) FROM flight_seat_reservations b
      WHERE b.seat_no IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM aircraft_seat_layouts p
                        WHERE p.aircraft_code = b.aircraft_code
                          AND p.seat_no       = b.seat_no))          AS missing_seat;


-- =====================================================================
-- Recovery test: rebuild the 8 original tables (plus the derived route
-- timetable) from the 3 source tables and compare them row by row with
-- the originals.
--
-- Each table gets two numbers:
--   lost  = rows in the original that the rebuild does not have
--   extra = rows in the rebuild that the original does not have
-- Both must be 0 for every table for the design to be lossless.
--
-- coordinates is cast to text because the point type has no equality
-- operator, which EXCEPT needs.
-- =====================================================================
WITH
src_routes AS (
    SELECT f.flight_no,
           f.departure_airport,
           f.arrival_airport,
           f.aircraft_code,
           f.scheduled_arrival - f.scheduled_departure AS duration,
           array_agg(DISTINCT to_char(f.scheduled_departure, 'ID')::integer
                     ORDER BY to_char(f.scheduled_departure, 'ID')::integer) AS days_of_week
    FROM flights f
    GROUP BY f.flight_no, f.departure_airport, f.arrival_airport,
             f.aircraft_code, f.scheduled_arrival - f.scheduled_departure
),
rb_bookings AS (
    SELECT DISTINCT book_ref, book_date, total_amount
    FROM flight_seat_reservations WHERE book_ref IS NOT NULL
),
rb_tickets AS (
    SELECT DISTINCT ticket_no, book_ref, passenger_id, passenger_name, contact_data
    FROM flight_seat_reservations WHERE ticket_no IS NOT NULL
),
rb_ticket_flights AS (
    SELECT ticket_no, flight_id, ticket_flights_fare_conditions, amount
    FROM flight_seat_reservations WHERE ticket_no IS NOT NULL
),
rb_boarding_passes AS (
    SELECT ticket_no, flight_id, boarding_no, seat_no
    FROM flight_seat_reservations WHERE ticket_no IS NOT NULL AND seat_no IS NOT NULL
),
rb_flights AS (
    SELECT DISTINCT flight_id, flight_no, scheduled_departure, scheduled_arrival,
           departure_airport, arrival_airport, status, aircraft_code,
           actual_departure, actual_arrival
    FROM flight_seat_reservations
),
rb_routes AS (
    SELECT DISTINCT flight_no, departure_airport, arrival_airport,
           aircraft_code, duration, days_of_week
    FROM flight_seat_reservations
),
rb_airports AS (
    SELECT airport_code, airport_name, city, coordinates::text, timezone
    FROM airport_sites
),
rb_aircrafts AS (
    SELECT DISTINCT aircraft_code, model, range
    FROM aircraft_seat_layouts
),
rb_seats AS (
    SELECT aircraft_code, seat_no, seats_fare_conditions
    FROM aircraft_seat_layouts WHERE seat_no IS NOT NULL
)
SELECT 'bookings' AS table_name,
       (SELECT count(*) FROM (SELECT book_ref, book_date, total_amount FROM bookings
                              EXCEPT SELECT * FROM rb_bookings) z) AS lost,
       (SELECT count(*) FROM (SELECT * FROM rb_bookings
                              EXCEPT SELECT book_ref, book_date, total_amount FROM bookings) z) AS extra
UNION ALL
SELECT 'tickets',
       (SELECT count(*) FROM (SELECT ticket_no, book_ref, passenger_id, passenger_name, contact_data FROM tickets
                              EXCEPT SELECT * FROM rb_tickets) z),
       (SELECT count(*) FROM (SELECT * FROM rb_tickets
                              EXCEPT SELECT ticket_no, book_ref, passenger_id, passenger_name, contact_data FROM tickets) z)
UNION ALL
SELECT 'ticket_flights',
       (SELECT count(*) FROM (SELECT ticket_no, flight_id, fare_conditions, amount FROM ticket_flights
                              EXCEPT SELECT * FROM rb_ticket_flights) z),
       (SELECT count(*) FROM (SELECT * FROM rb_ticket_flights
                              EXCEPT SELECT ticket_no, flight_id, fare_conditions, amount FROM ticket_flights) z)
UNION ALL
SELECT 'boarding_passes',
       (SELECT count(*) FROM (SELECT ticket_no, flight_id, boarding_no, seat_no FROM boarding_passes
                              EXCEPT SELECT * FROM rb_boarding_passes) z),
       (SELECT count(*) FROM (SELECT * FROM rb_boarding_passes
                              EXCEPT SELECT ticket_no, flight_id, boarding_no, seat_no FROM boarding_passes) z)
UNION ALL
SELECT 'flights',
       (SELECT count(*) FROM (SELECT flight_id, flight_no, scheduled_departure, scheduled_arrival,
                                     departure_airport, arrival_airport, status, aircraft_code,
                                     actual_departure, actual_arrival FROM flights
                              EXCEPT SELECT * FROM rb_flights) z),
       (SELECT count(*) FROM (SELECT * FROM rb_flights
                              EXCEPT SELECT flight_id, flight_no, scheduled_departure, scheduled_arrival,
                                            departure_airport, arrival_airport, status, aircraft_code,
                                            actual_departure, actual_arrival FROM flights) z)
UNION ALL
SELECT 'routes',
       (SELECT count(*) FROM (SELECT * FROM src_routes
                              EXCEPT SELECT * FROM rb_routes) z),
       (SELECT count(*) FROM (SELECT * FROM rb_routes
                              EXCEPT SELECT * FROM src_routes) z)
UNION ALL
SELECT 'airports_data',
       (SELECT count(*) FROM (SELECT airport_code, airport_name, city, coordinates::text, timezone
                              FROM airports_data
                              EXCEPT SELECT * FROM rb_airports) z),
       (SELECT count(*) FROM (SELECT * FROM rb_airports
                              EXCEPT SELECT airport_code, airport_name, city, coordinates::text, timezone
                              FROM airports_data) z)
UNION ALL
SELECT 'aircrafts_data',
       (SELECT count(*) FROM (SELECT aircraft_code, model, range FROM aircrafts_data
                              EXCEPT SELECT * FROM rb_aircrafts) z),
       (SELECT count(*) FROM (SELECT * FROM rb_aircrafts
                              EXCEPT SELECT aircraft_code, model, range FROM aircrafts_data) z)
UNION ALL
SELECT 'seats',
       (SELECT count(*) FROM (SELECT aircraft_code, seat_no, fare_conditions FROM seats
                              EXCEPT SELECT * FROM rb_seats) z),
       (SELECT count(*) FROM (SELECT * FROM rb_seats
                              EXCEPT SELECT aircraft_code, seat_no, fare_conditions FROM seats) z);


-- =====================================================================
-- Export (run in psql)
-- =====================================================================
-- \copy flight_seat_reservations TO 'flight_seat_reservations.csv' WITH (FORMAT csv, HEADER)
-- \copy airport_sites            TO 'airport_sites.csv'            WITH (FORMAT csv, HEADER)
-- \copy aircraft_seat_layouts    TO 'aircraft_seat_layouts.csv'    WITH (FORMAT csv, HEADER)
