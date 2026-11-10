"""Replays archive bookings onto Kafka's booking.created topic, in their
real book_date order and relative timing (scaled by SPEED_FACTOR)."""

import json
import logging
import os
import time

import psycopg2
from kafka import KafkaProducer
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PG_HOST = os.environ.get("PG_HOST", "db")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DATABASE = os.environ.get("PG_DATABASE", "demo")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "123456")
PG_SOURCE_SCHEMA = os.environ.get("PG_SOURCE_SCHEMA", "archive")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka-0:9092")
TOPIC = os.environ.get("BOOKING_TOPIC", "booking.created")

SPEED_FACTOR = float(os.environ.get("SPEED_FACTOR", "4000"))  # 1 real second = this many simulated seconds
MAX_GAP_SECONDS = float(os.environ.get("MAX_GAP_SECONDS", "2.0"))  # cap on any single pause
MIN_GAP_SECONDS = float(os.environ.get("MIN_GAP_SECONDS", "0.05"))  # floor, so same-timestamp bursts don't fire unthrottled
LIMIT_BOOKINGS = os.environ.get("LIMIT_BOOKINGS", "")  # for smoke-testing

QUERY_TEMPLATE = """
SELECT book_ref, book_date, total_amount,
       jsonb_agg(jsonb_build_object(
           'ticket_no', ticket_no,
           'passenger_id', passenger_id,
           'passenger_name', passenger_name,
           'flight_id', flight_id,
           'flight_no', flight_no,
           'departure_airport', departure_airport,
           'arrival_airport', arrival_airport,
           'aircraft_code', aircraft_code,
           'scheduled_departure', scheduled_departure,
           'scheduled_arrival', scheduled_arrival,
           'fare_conditions', ticket_flights_fare_conditions,
           'amount', amount
       ) ORDER BY flight_id) AS segments
FROM {schema}.flight_seat_reservations
WHERE book_ref IS NOT NULL
GROUP BY book_ref, book_date, total_amount
ORDER BY book_date
{limit_clause};
"""


def fetch_bookings(conn):
    limit_clause = f"LIMIT {int(LIMIT_BOOKINGS)}" if LIMIT_BOOKINGS else ""
    query = QUERY_TEMPLATE.format(schema=PG_SOURCE_SCHEMA, limit_clause=limit_clause)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        return cur.fetchall()


def main() -> None:
    logger.info("Connecting to Postgres %s:%s/%s (schema=%s)...", PG_HOST, PG_PORT, PG_DATABASE, PG_SOURCE_SCHEMA)
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE, user=PG_USER, password=PG_PASSWORD,
    )
    try:
        bookings = fetch_bookings(conn)
    finally:
        conn.close()
    logger.info("Loaded %d bookings; replaying onto topic %s (speed_factor=%s, bootstrap=%s)",
                len(bookings), TOPIC, SPEED_FACTOR, KAFKA_BOOTSTRAP)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    prev_book_date = None
    sent = 0
    try:
        for row in bookings:
            book_date = row["book_date"]
            if prev_book_date is not None:
                real_gap = (book_date - prev_book_date).total_seconds()
                sleep_for = min(max(real_gap / SPEED_FACTOR, MIN_GAP_SECONDS), MAX_GAP_SECONDS)
                time.sleep(sleep_for)
            prev_book_date = book_date

            payload = {
                "book_ref": row["book_ref"],
                "book_date": row["book_date"].isoformat(),
                "total_amount": float(row["total_amount"]) if row["total_amount"] is not None else None,
                "segments": row["segments"],
            }
            producer.send(TOPIC, key=row["book_ref"], value=payload)
            sent += 1
            if sent % 1000 == 0:
                logger.info("Sent %d/%d bookings (last book_date=%s)", sent, len(bookings), book_date)
    finally:
        producer.flush()
        producer.close()

    logger.info("Done. Sent %d bookings total.", sent)


if __name__ == "__main__":
    main()
