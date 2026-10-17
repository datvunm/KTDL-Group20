-- source/schema.sql
-- Creates empty bookings schema with 8 tables, constraints, indexes,
-- sim_state table, and STABLE bookings.now() function.
-- Unqualified references in demo db resolve to bookings schema.
-- Simulator SQL qualifies archive. and bookings. explicitly.

CREATE SCHEMA IF NOT EXISTS bookings;

-- 1. aircrafts_data
CREATE TABLE IF NOT EXISTS bookings.aircrafts_data (
    aircraft_code char(3) PRIMARY KEY,
    model jsonb NOT NULL,
    range integer NOT NULL CHECK (range > 0)
);

-- 2. airports_data
CREATE TABLE IF NOT EXISTS bookings.airports_data (
    airport_code char(3) PRIMARY KEY,
    airport_name jsonb NOT NULL,
    city jsonb NOT NULL,
    coordinates point NOT NULL,
    timezone text NOT NULL
);

-- 3. seats
CREATE TABLE IF NOT EXISTS bookings.seats (
    aircraft_code char(3) NOT NULL,
    seat_no varchar(4) NOT NULL,
    fare_conditions varchar(10) NOT NULL CHECK (fare_conditions IN ('Economy', 'Comfort', 'Business')),
    PRIMARY KEY (aircraft_code, seat_no)
);

-- 4. bookings
CREATE TABLE IF NOT EXISTS bookings.bookings (
    book_ref char(6) PRIMARY KEY,
    book_date timestamptz NOT NULL,
    total_amount numeric(10,2) NOT NULL
);

-- 5. tickets
CREATE TABLE IF NOT EXISTS bookings.tickets (
    ticket_no char(13) PRIMARY KEY,
    book_ref char(6) NOT NULL,
    passenger_id varchar(20) NOT NULL,
    passenger_name text NOT NULL,
    contact_data jsonb
);

-- 6. flights
CREATE TABLE IF NOT EXISTS bookings.flights (
    flight_id integer PRIMARY KEY,
    flight_no char(6) NOT NULL,
    scheduled_departure timestamptz NOT NULL,
    scheduled_arrival timestamptz NOT NULL,
    departure_airport char(3) NOT NULL,
    arrival_airport char(3) NOT NULL,
    status varchar(20) NOT NULL CHECK (status IN ('On Time', 'Delayed', 'Departed', 'Arrived', 'Scheduled', 'Cancelled')),
    aircraft_code char(3) NOT NULL,
    actual_departure timestamptz,
    actual_arrival timestamptz,
    CONSTRAINT flights_flight_no_scheduled_departure_key UNIQUE (flight_no, scheduled_departure),
    CONSTRAINT flights_check_arrival_after_departure CHECK (scheduled_arrival > scheduled_departure),
    CONSTRAINT flights_check_actual_arrival CHECK (actual_arrival IS NULL OR (actual_departure IS NOT NULL AND actual_arrival > actual_departure))
);

-- 7. ticket_flights
CREATE TABLE IF NOT EXISTS bookings.ticket_flights (
    ticket_no char(13) NOT NULL,
    flight_id integer NOT NULL,
    fare_conditions varchar(10) NOT NULL CHECK (fare_conditions IN ('Economy', 'Comfort', 'Business')),
    amount numeric(10,2) NOT NULL CHECK (amount >= 0),
    PRIMARY KEY (ticket_no, flight_id)
);

-- 8. boarding_passes
CREATE TABLE IF NOT EXISTS bookings.boarding_passes (
    ticket_no char(13) NOT NULL,
    flight_id integer NOT NULL,
    boarding_no integer NOT NULL,
    seat_no varchar(4) NOT NULL,
    PRIMARY KEY (ticket_no, flight_id),
    CONSTRAINT boarding_passes_flight_boarding_no_key UNIQUE (flight_id, boarding_no),
    CONSTRAINT boarding_passes_flight_seat_no_key UNIQUE (flight_id, seat_no)
);

-- Indexes for performance (no foreign keys as per contract)
CREATE INDEX IF NOT EXISTS ticket_flights_flight_id_idx ON bookings.ticket_flights (flight_id);
CREATE INDEX IF NOT EXISTS boarding_passes_flight_id_idx ON bookings.boarding_passes (flight_id);
CREATE INDEX IF NOT EXISTS bookings_book_date_idx ON bookings.bookings (book_date);
CREATE INDEX IF NOT EXISTS tickets_book_ref_idx ON bookings.tickets (book_ref);

-- Simulator state tracking current simulation cutoff
CREATE TABLE IF NOT EXISTS bookings.sim_state (
    cutoff timestamptz NOT NULL
);

-- Initialize default cutoff if table is empty (2017-04-01 before earliest booking in dump)
INSERT INTO bookings.sim_state (cutoff)
SELECT '2017-04-01 00:00:00+03'::timestamptz
WHERE NOT EXISTS (SELECT 1 FROM bookings.sim_state);

-- STABLE function (provolatile = 's') returning current simulation cutoff.
-- STABLE ensures planner never folds stale values while allowing query optimization.
CREATE OR REPLACE FUNCTION bookings.now()
RETURNS timestamptz
LANGUAGE sql
STABLE
AS $$
    SELECT cutoff FROM bookings.sim_state LIMIT 1;
$$;

-- Ensure unqualified references in demo database resolve to bookings schema
ALTER DATABASE demo SET search_path = bookings, public;
