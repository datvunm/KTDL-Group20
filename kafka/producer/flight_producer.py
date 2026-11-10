"""Replays real flight departure/arrival timestamps onto Kafka's
flight.departed / flight.arrived topics, paced like booking_producer.py.
Skips flights with no actual_departure (Scheduled/Cancelled) - archive has
no timestamp for a cancellation, so there's nothing real to replay."""

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
DEPARTED_TOPIC = os.environ.get("DEPARTED_TOPIC", "flight.departed")
ARRIVED_TOPIC = os.environ.get("ARRIVED_TOPIC", "flight.arrived")

SPEED_FACTOR = float(os.environ.get("SPEED_FACTOR", "4000"))
MAX_GAP_SECONDS = float(os.environ.get("MAX_GAP_SECONDS", "2.0"))
MIN_GAP_SECONDS = float(os.environ.get("MIN_GAP_SECONDS", "0.05"))  # floor, so same-timestamp bursts don't fire unthrottled
LIMIT_FLIGHTS = os.environ.get("LIMIT_FLIGHTS", "")

QUERY_TEMPLATE = """
SELECT DISTINCT flight_id, flight_no, scheduled_departure, scheduled_arrival,
       departure_airport, arrival_airport, status, aircraft_code,
       actual_departure, actual_arrival
FROM {schema}.flight_seat_reservations
WHERE flight_id IS NOT NULL
  AND actual_departure IS NOT NULL
ORDER BY actual_departure
{limit_clause};
"""


def fetch_flights(conn):
    limit_clause = f"LIMIT {int(LIMIT_FLIGHTS)}" if LIMIT_FLIGHTS else ""
    query = QUERY_TEMPLATE.format(schema=PG_SOURCE_SCHEMA, limit_clause=limit_clause)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        return cur.fetchall()


def build_events(flights):
    """Expand each flight into up to two time-ordered (timestamp, topic, payload) events."""
    events = []
    for f in flights:
        base = {
            "flight_id": f["flight_id"],
            "flight_no": f["flight_no"],
            "scheduled_departure": f["scheduled_departure"].isoformat(),
            "scheduled_arrival": f["scheduled_arrival"].isoformat(),
            "departure_airport": f["departure_airport"],
            "arrival_airport": f["arrival_airport"],
            "aircraft_code": f["aircraft_code"],
        }
        departed_payload = dict(
            base, status=f["status"], actual_departure=f["actual_departure"].isoformat(), actual_arrival=None
        )
        events.append((f["actual_departure"], DEPARTED_TOPIC, departed_payload))

        if f["actual_arrival"] is not None:
            arrived_payload = dict(
                base,
                status=f["status"],
                actual_departure=f["actual_departure"].isoformat(),
                actual_arrival=f["actual_arrival"].isoformat(),
            )
            events.append((f["actual_arrival"], ARRIVED_TOPIC, arrived_payload))

    events.sort(key=lambda e: e[0])
    return events


def main() -> None:
    logger.info("Connecting to Postgres %s:%s/%s (schema=%s)...", PG_HOST, PG_PORT, PG_DATABASE, PG_SOURCE_SCHEMA)
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE, user=PG_USER, password=PG_PASSWORD)
    try:
        flights = fetch_flights(conn)
    finally:
        conn.close()

    events = build_events(flights)
    logger.info(
        "Loaded %d flights -> %d departure/arrival events; replaying (speed_factor=%s, bootstrap=%s)",
        len(flights), len(events), SPEED_FACTOR, KAFKA_BOOTSTRAP,
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    prev_ts = None
    sent = 0
    try:
        for ts, topic, payload in events:
            if prev_ts is not None:
                real_gap = (ts - prev_ts).total_seconds()
                sleep_for = min(max(real_gap / SPEED_FACTOR, MIN_GAP_SECONDS), MAX_GAP_SECONDS)
                time.sleep(sleep_for)
            prev_ts = ts

            producer.send(topic, key=payload["flight_id"], value=payload)
            sent += 1
            if sent % 1000 == 0:
                logger.info("Sent %d/%d events (last ts=%s)", sent, len(events), ts)
    finally:
        producer.flush()
        producer.close()

    logger.info("Done. Sent %d events total.", sent)


if __name__ == "__main__":
    main()
