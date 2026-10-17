-- source/tests/synthetic_dump.sql
-- Synthetic raw dump mimicking source/data/raw (3 denormalized tables in
-- schema archive, full history). Minimal dataset to thoroughly test
-- simulation cutoffs and rules. Rows are the output of source/build_raw.sql
-- over this normalized scenario:
--
-- Aircraft 773 (seats 1A Business, 2A Economy), CN1 (seats 1A, 1B Economy).
-- Flights covering every status:
--   Flight 1: Arrived   (scheduled 2017-06-10 10:00, actuals on-time)
--   Flight 2: Departed  (scheduled 2017-08-15 17:00, actual_departure 17:05, actual_arrival NULL)
--   Flight 3: Delayed   (scheduled 2017-08-15 20:00, actuals NULL, no bookings)
--   Flight 4: On Time   (scheduled 2017-08-15 22:00, actuals NULL)
--   Flight 5: Scheduled (scheduled 2017-08-20 10:00, actuals NULL)
--   Flight 6: Cancelled (scheduled 2017-07-01 10:00, actuals NULL)
-- Bookings (one ticket each):
--   B00001 2017-05-01 -> flight 1 (> 30 days after booking), seat 2A
--   B00002 2017-08-10 -> flight 2, seat 1A
--   B00003 2017-08-15 -> flight 4, seat 1A
--   B00004 2017-06-20 -> flight 6 (cancelled), no seat
--   B00005 2017-08-14 -> flight 5 (check-in not open at archive end), no seat

CREATE SCHEMA archive;

CREATE TABLE archive.aircraft_seat_layouts (
    aircraft_code character(3),
    model jsonb,
    range integer,
    seat_no character varying(4),
    seats_fare_conditions character varying(10)
);

CREATE TABLE archive.airport_sites (
    airport_code character(3),
    airport_name jsonb,
    city jsonb,
    coordinates point,
    timezone text
);

CREATE TABLE archive.flight_seat_reservations (
    flight_id integer,
    flight_no character(6),
    status character varying(20),
    scheduled_departure timestamp with time zone,
    scheduled_arrival timestamp with time zone,
    actual_departure timestamp with time zone,
    actual_arrival timestamp with time zone,
    departure_airport character(3),
    arrival_airport character(3),
    aircraft_code character(3),
    days_of_week integer[],
    duration interval,
    seat_no character varying,
    boarding_no integer,
    ticket_no character(13),
    ticket_flights_fare_conditions character varying(10),
    amount numeric(10,2),
    book_ref character(6),
    passenger_id character varying(20),
    passenger_name text,
    contact_data jsonb,
    book_date timestamp with time zone,
    total_amount numeric(10,2)
);

INSERT INTO archive.aircraft_seat_layouts VALUES
('773', '{"en": "Boeing 777-300", "ru": "Боинг 777-300"}', 11100, '1A', 'Business'),
('773', '{"en": "Boeing 777-300", "ru": "Боинг 777-300"}', 11100, '2A', 'Economy'),
('CN1', '{"en": "Cessna 208 Caravan", "ru": "Сессна 208 Караван"}', 1200, '1A', 'Economy'),
('CN1', '{"en": "Cessna 208 Caravan", "ru": "Сессна 208 Караван"}', 1200, '1B', 'Economy');

INSERT INTO archive.airport_sites VALUES
('SVO', '{"en": "Sheremetyevo", "ru": "Шереметьево"}', '{"en": "Moscow", "ru": "Москва"}', '(37.4146,55.9726)', 'Europe/Moscow'),
('LED', '{"en": "Pulkovo", "ru": "Пулково"}', '{"en": "St. Petersburg", "ru": "Санкт-Петербург"}', '(30.2625,59.8003)', 'Europe/Moscow'),
('OVB', '{"en": "Tolmachevo", "ru": "Толмачево"}', '{"en": "Novosibirsk", "ru": "Новосибирск"}', '(82.6507,55.0126)', 'Asia/Novosibirsk');

-- Columns: flight (10), route (2), seat_no, boarding_no, ticket (3), passenger (4), booking (2)
INSERT INTO archive.flight_seat_reservations VALUES
-- Flight 1: Arrived; 1A empty, 2A seated (B00001)
(1, 'PG0001', 'Arrived', '2017-06-10 10:00:00+03', '2017-06-10 11:30:00+03', '2017-06-10 10:00:00+03', '2017-06-10 11:30:00+03', 'SVO', 'LED', '773', '{6}', '01:30:00',
 '1A', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(1, 'PG0001', 'Arrived', '2017-06-10 10:00:00+03', '2017-06-10 11:30:00+03', '2017-06-10 10:00:00+03', '2017-06-10 11:30:00+03', 'SVO', 'LED', '773', '{6}', '01:30:00',
 '2A', 1, '0005432000001', 'Economy', 5000.00, 'B00001', '1111 222222', 'IVAN IVANOV', '{"phone": "+70000000001"}', '2017-05-01 10:00:00+03', 5000.00),
-- Flight 2: Departed; 1A seated (B00002), 2A empty
(2, 'PG0002', 'Departed', '2017-08-15 17:00:00+03', '2017-08-15 18:30:00+03', '2017-08-15 17:05:00+03', NULL, 'LED', 'SVO', '773', '{2}', '01:30:00',
 '1A', 1, '0005432000002', 'Business', 7000.00, 'B00002', '3333 444444', 'PETR PETROV', '{"phone": "+70000000002"}', '2017-08-10 10:00:00+03', 7000.00),
(2, 'PG0002', 'Departed', '2017-08-15 17:00:00+03', '2017-08-15 18:30:00+03', '2017-08-15 17:05:00+03', NULL, 'LED', 'SVO', '773', '{2}', '01:30:00',
 '2A', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
-- Flight 3: Delayed; both seats empty
(3, 'PG0003', 'Delayed', '2017-08-15 20:00:00+03', '2017-08-16 01:00:00+03', NULL, NULL, 'SVO', 'OVB', 'CN1', '{2}', '05:00:00',
 '1A', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(3, 'PG0003', 'Delayed', '2017-08-15 20:00:00+03', '2017-08-16 01:00:00+03', NULL, NULL, 'SVO', 'OVB', 'CN1', '{2}', '05:00:00',
 '1B', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
-- Flight 4: On Time; 1A seated (B00003), 1B empty
(4, 'PG0004', 'On Time', '2017-08-15 22:00:00+03', '2017-08-16 03:00:00+03', NULL, NULL, 'OVB', 'SVO', 'CN1', '{2}', '05:00:00',
 '1A', 1, '0005432000003', 'Economy', 3000.00, 'B00003', '5555 666666', 'SERGEY SERGEEV', '{"phone": "+70000000003"}', '2017-08-15 12:00:00+03', 3000.00),
(4, 'PG0004', 'On Time', '2017-08-15 22:00:00+03', '2017-08-16 03:00:00+03', NULL, NULL, 'OVB', 'SVO', 'CN1', '{2}', '05:00:00',
 '1B', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
-- Flight 5: Scheduled; both seats empty, B00005 booked without seat
(5, 'PG0005', 'Scheduled', '2017-08-20 10:00:00+03', '2017-08-20 11:30:00+03', NULL, NULL, 'SVO', 'LED', 'CN1', '{7}', '01:30:00',
 '1A', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(5, 'PG0005', 'Scheduled', '2017-08-20 10:00:00+03', '2017-08-20 11:30:00+03', NULL, NULL, 'SVO', 'LED', 'CN1', '{7}', '01:30:00',
 '1B', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(5, 'PG0005', 'Scheduled', '2017-08-20 10:00:00+03', '2017-08-20 11:30:00+03', NULL, NULL, 'SVO', 'LED', 'CN1', '{7}', '01:30:00',
 NULL, NULL, '0005432000005', 'Economy', 3500.00, 'B00005', '9999 000000', 'ELENA ELENOVA', '{"phone": "+70000000005"}', '2017-08-14 10:00:00+03', 3500.00),
-- Flight 6: Cancelled; both seats empty, B00004 booked without seat
(6, 'PG0006', 'Cancelled', '2017-07-01 10:00:00+03', '2017-07-01 11:30:00+03', NULL, NULL, 'SVO', 'LED', '773', '{6}', '01:30:00',
 '1A', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(6, 'PG0006', 'Cancelled', '2017-07-01 10:00:00+03', '2017-07-01 11:30:00+03', NULL, NULL, 'SVO', 'LED', '773', '{6}', '01:30:00',
 '2A', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(6, 'PG0006', 'Cancelled', '2017-07-01 10:00:00+03', '2017-07-01 11:30:00+03', NULL, NULL, 'SVO', 'LED', '773', '{6}', '01:30:00',
 NULL, NULL, '0005432000004', 'Economy', 4000.00, 'B00004', '7777 888888', 'ANNA ANNOVA', '{"phone": "+70000000004"}', '2017-06-20 10:00:00+03', 4000.00);
