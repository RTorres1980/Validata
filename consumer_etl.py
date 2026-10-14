#!/usr/bin/env python3
"""consumer_etl.py
Consumes events from Kafka, validates and flattens them, and writes them to Postgres
in batches. Invalid events are routed to a dead-letter topic (<topic>.dlq) with the
rejection reason attached, so nothing is silently dropped.

Usage: python consumer_etl.py --topic events
"""
import argparse
import json
import os

import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from psycopg2.extras import execute_values

from transform import to_row, validate_event

POSTGRES_CONN = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "events_db"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}

TABLE_SCHEMA = """CREATE TABLE IF NOT EXISTS public.events_processed (
    event_id TEXT PRIMARY KEY,
    user_id INT,
    user_tenure_days INT,
    action TEXT,
    amount NUMERIC,
    source TEXT,
    ts TIMESTAMP
);"""

INSERT_SQL = (
    "INSERT INTO public.events_processed "
    "(event_id, user_id, user_tenure_days, action, amount, source, ts) VALUES %s "
    "ON CONFLICT (event_id) DO NOTHING"
)


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(TABLE_SCHEMA)
    conn.commit()


def write_batch(conn, rows):
    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows)
    conn.commit()


def main(topic="events", bootstrap="localhost:9092", group="etl-group", batch_size=100, idle_timeout=None):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        # When set, stop after N idle seconds so scheduled (Airflow) runs terminate.
        consumer_timeout_ms=int(idle_timeout * 1000) if idle_timeout else float("inf"),
    )
    dlq = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    conn = psycopg2.connect(**POSTGRES_CONN)
    ensure_table(conn)

    batch, rejected = [], 0
    print(f"Consuming topic={topic} (dead letters -> {topic}.dlq)")
    try:
        for msg in consumer:
            rec = msg.value
            valid, reason = validate_event(rec)
            if not valid:
                rejected += 1
                dlq.send(f"{topic}.dlq", {"reason": reason, "event": rec})
                continue
            batch.append(to_row(rec))
            if len(batch) >= batch_size:
                write_batch(conn, batch)
                consumer.commit()  # commit offsets only after the write succeeds
                print(f"Wrote {len(batch)} rows (rejected so far: {rejected})")
                batch = []
    except KeyboardInterrupt:
        print("Stopping consumer")
    finally:
        if batch:
            write_batch(conn, batch)
            consumer.commit()
            print(f"Wrote final {len(batch)} rows")
        print(f"Rejected {rejected} events to {topic}.dlq")
        dlq.flush()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", default="events")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--group", default="etl-group")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--idle-timeout", type=float, help="exit after this many idle seconds")
    args = parser.parse_args()
    main(args.topic, args.bootstrap, args.group, args.batch_size, args.idle_timeout)
