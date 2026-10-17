-- source/tests/synthetic_dump.sql
-- Synthetic dump file mimicking the official Airlines demo dump prologue & schema.
-- Contains minimal dataset to thoroughly test simulation cutoffs and rules.

DROP DATABASE demo;
CREATE DATABASE demo;
\connect demo

CREATE SCHEMA bookings;

CREATE OR REPLACE FUNCTION bookings.now()
RETURNS timestamptz
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT '2017-08-15 18:00:00+03'::timestamptz;
$$;

CREATE TABLE bookings.aircrafts_data (
    aircraft_code char(3) PRIMARY KEY,
    model jsonb NOT NULL,
    range integer NOT NULL CHECK (range > 0)
);

CREATE TABLE bookings.airports_data (
    airport_code char(3) PRIMARY KEY,
    airport_name jsonb NOT NULL,
    city jsonb NOT NULL,
    coordinates point NOT NULL,
    timezone text NOT NULL
);

CREATE TABLE bookings.seats (
    aircraft_code char(3) NOT NULL,
    seat_no varchar(4) NOT NULL,
    fare_conditions varchar(10) NOT NULL CHECK (fare_conditions IN ('Economy', 'Comfort', 'Business')),
    PRIMARY KEY (aircraft_code, seat_no)
);

CREATE TABLE bookings.bookings (
    book_ref char(6) PRIMARY KEY,
    book_date timestamptz NOT NULL,
    total_amount numeric(10,2) NOT NULL
);

CREATE TABLE bookings.tickets (
    ticket_no char(13) PRIMARY KEY,
    book_ref char(6) NOT NULL,
    passenger_id varchar(20) NOT NULL,
    passenger_name text NOT NULL,
    contact_data jsonb
);

CREATE TABLE bookings.flights (
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

CREATE TABLE bookings.ticket_flights (
    ticket_no char(13) NOT NULL,
    flight_id integer NOT NULL,
    fare_conditions varchar(10) NOT NULL CHECK (fare_conditions IN ('Economy', 'Comfort', 'Business')),
    amount numeric(10,2) NOT NULL CHECK (amount >= 0),
    PRIMARY KEY (ticket_no, flight_id)
);

CREATE TABLE bookings.boarding_passes (
    ticket_no char(13) NOT NULL,
    flight_id integer NOT NULL,
    boarding_no integer NOT NULL,
    seat_no varchar(4) NOT NULL,
    PRIMARY KEY (ticket_no, flight_id),
    CONSTRAINT boarding_passes_flight_boarding_no_key UNIQUE (flight_id, boarding_no),
    CONSTRAINT boarding_passes_flight_seat_no_key UNIQUE (flight_id, seat_no)
);

ALTER DATABASE demo SET search_path = bookings, public;

-- Populate synthetic data
INSERT INTO bookings.aircrafts_data VALUES
('773', '{"en": "Boeing 777-300", "ru": "Боинг 777-300"}'::jsonb, 11100),
('CN1', '{"en": "Cessna 208 Caravan", "ru": "Сессна 208 Караван"}'::jsonb, 1200);

INSERT INTO bookings.airports_data VALUES
('SVO', '{"en": "Sheremetyevo", "ru": "Шереметьево"}'::jsonb, '{"en": "Moscow", "ru": "Москва"}'::jsonb, '(37.4146, 55.9726)'::point, 'Europe/Moscow'),
('LED', '{"en": "Pulkovo", "ru": "Пулково"}'::jsonb, '{"en": "St. Petersburg", "ru": "Санкт-Петербург"}'::jsonb, '(30.2625, 59.8003)'::point, 'Europe/Moscow'),
('OVB', '{"en": "Tolmachevo", "ru": "Толмачево"}'::jsonb, '{"en": "Novosibirsk", "ru": "Новосибирск"}'::jsonb, '(82.6507, 55.0126)'::point, 'Asia/Novosibirsk');

INSERT INTO bookings.seats VALUES
('773', '1A', 'Business'),
('773', '2A', 'Economy'),
('CN1', '1A', 'Economy'),
('CN1', '1B', 'Economy');

-- Flights covering every status:
-- Flight 1: Arrived (scheduled 2017-06-10 10:00, actuals on-time)
-- Flight 2: Departed (scheduled 2017-08-15 17:00, actual_departure 17:05, actual_arrival NULL)
-- Flight 3: Delayed (scheduled 2017-08-15 20:00, actuals NULL)
-- Flight 4: On Time (scheduled 2017-08-15 22:00, actuals NULL)
-- Flight 5: Scheduled (scheduled 2017-08-20 10:00, actuals NULL)
-- Flight 6: Cancelled (scheduled 2017-07-01 10:00, actuals NULL)
INSERT INTO bookings.flights VALUES
(1, 'PG0001', '2017-06-10 10:00:00+03', '2017-06-10 11:30:00+03', 'SVO', 'LED', 'Arrived',   '773', '2017-06-10 10:00:00+03', '2017-06-10 11:30:00+03'),
(2, 'PG0002', '2017-08-15 17:00:00+03', '2017-08-15 18:30:00+03', 'LED', 'SVO', 'Departed',  '773', '2017-08-15 17:05:00+03', NULL),
(3, 'PG0003', '2017-08-15 20:00:00+03', '2017-08-16 01:00:00+03', 'SVO', 'OVB', 'Delayed',   'CN1', NULL, NULL),
(4, 'PG0004', '2017-08-15 22:00:00+03', '2017-08-16 03:00:00+03', 'OVB', 'SVO', 'On Time',   'CN1', NULL, NULL),
(5, 'PG0005', '2017-08-20 10:00:00+03', '2017-08-20 11:30:00+03', 'SVO', 'LED', 'Scheduled', 'CN1', NULL, NULL),
(6, 'PG0006', '2017-07-01 10:00:00+03', '2017-07-01 11:30:00+03', 'SVO', 'LED', 'Cancelled', '773', NULL, NULL);

-- Bookings:
-- Booking B00001 is on 2017-05-01, flight 1 is on 2017-06-10 (> 30 days after booking)
INSERT INTO bookings.bookings VALUES
('B00001', '2017-05-01 10:00:00+03', 5000.00),
('B00002', '2017-08-10 10:00:00+03', 7000.00),
('B00003', '2017-08-15 12:00:00+03', 3000.00),
('B00004', '2017-06-20 10:00:00+03', 4000.00),
('B00005', '2017-08-14 10:00:00+03', 3500.00);

INSERT INTO bookings.tickets VALUES
('0005432000001', 'B00001', '1111 222222', 'IVAN IVANOV',     '{"phone": "+70000000001"}'::jsonb),
('0005432000002', 'B00002', '3333 444444', 'PETR PETROV',     '{"phone": "+70000000002"}'::jsonb),
('0005432000003', 'B00003', '5555 666666', 'SERGEY SERGEEV', '{"phone": "+70000000003"}'::jsonb),
('0005432000004', 'B00004', '7777 888888', 'ANNA ANNOVA',     '{"phone": "+70000000004"}'::jsonb),
('0005432000005', 'B00005', '9999 000000', 'ELENA ELENOVA',   '{"phone": "+70000000005"}'::jsonb);

INSERT INTO bookings.ticket_flights VALUES
('0005432000001', 1, 'Economy',  5000.00),
('0005432000002', 2, 'Business', 7000.00),
('0005432000003', 4, 'Economy',  3000.00),
('0005432000004', 6, 'Economy',  4000.00),
('0005432000005', 5, 'Economy',  3500.00);

-- Boarding passes in baseline archive:
-- Flight 1 (arrived): boarding pass issued
-- Flight 2 (departed): boarding pass issued
-- Flight 4 (on time within 24h of now): boarding pass issued
-- Flight 5 (> 24h away): check-in not open, no pass
-- Flight 6 (cancelled): no pass
INSERT INTO bookings.boarding_passes VALUES
('0005432000001', 1, 1, '2A'),
('0005432000002', 2, 1, '1A'),
('0005432000003', 4, 1, '1A');
