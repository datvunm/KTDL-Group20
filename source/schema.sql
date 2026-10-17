-- source/schema.sql
-- Creates empty raw schema with the 3 denormalized source tables, the
-- sim_state table, and STABLE raw.now().
-- archive.* (loaded from source/data/raw) holds the same 3 tables for the
-- full history; simulate.sql fills raw.* with the state as of a cutoff.
-- Unqualified references in demo db resolve to raw schema.

CREATE SCHEMA IF NOT EXISTS raw;

-- 1. flight_seat_reservations
-- Grain: one row per seat per flight, plus one row per booked passenger
-- who has not checked in yet (no seat assigned).
CREATE TABLE IF NOT EXISTS raw.flight_seat_reservations (
    flight_id integer NOT NULL,
    flight_no char(6) NOT NULL,
    status varchar(20) NOT NULL,
    scheduled_departure timestamptz NOT NULL,
    scheduled_arrival timestamptz NOT NULL,
    actual_departure timestamptz,
    actual_arrival timestamptz,
    departure_airport char(3) NOT NULL,
    arrival_airport char(3) NOT NULL,
    aircraft_code char(3) NOT NULL,
    days_of_week integer[],
    duration interval,
    seat_no varchar(4),
    boarding_no integer,
    ticket_no char(13),
    ticket_flights_fare_conditions varchar(10),
    amount numeric(10,2),
    book_ref char(6),
    passenger_id varchar(20),
    passenger_name text,
    contact_data jsonb,
    book_date timestamptz,
    total_amount numeric(10,2)
);

-- 2. airport_sites
CREATE TABLE IF NOT EXISTS raw.airport_sites (
    airport_code char(3) NOT NULL,
    airport_name jsonb,
    city jsonb,
    coordinates point,
    timezone text
);

-- 3. aircraft_seat_layouts
CREATE TABLE IF NOT EXISTS raw.aircraft_seat_layouts (
    aircraft_code char(3) NOT NULL,
    model jsonb,
    range integer,
    seat_no varchar(4),
    seats_fare_conditions varchar(10)
);

-- Bronze reads flight_seat_reservations in flight_id ranges
CREATE INDEX IF NOT EXISTS flight_seat_reservations_flight_id_idx ON raw.flight_seat_reservations (flight_id);

-- Simulator state tracking current simulation cutoff
CREATE TABLE IF NOT EXISTS raw.sim_state (
    cutoff timestamptz NOT NULL
);

-- Initialize default cutoff if table is empty (2017-04-01 before earliest booking in dump)
INSERT INTO raw.sim_state (cutoff)
SELECT '2017-04-01 00:00:00+03'::timestamptz
WHERE NOT EXISTS (SELECT 1 FROM raw.sim_state);

-- STABLE function (provolatile = 's') returning current simulation cutoff.
-- STABLE ensures planner never folds stale values while allowing query optimization.
CREATE OR REPLACE FUNCTION raw.now()
RETURNS timestamptz
LANGUAGE sql
STABLE
AS $$
    SELECT cutoff FROM raw.sim_state LIMIT 1;
$$;

-- Ensure unqualified references in demo database resolve to raw schema
ALTER DATABASE demo SET search_path = raw, public;
