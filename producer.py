#!/usr/bin/env python3
"""producer.py
Simple Kafka event producer that simulates user events for a streaming pipeline.
Usage: python producer.py --topic events --rate 5
"""
import argparse
import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer


def random_event():
    user_id = random.randint(1, 200)
    return {
        "event_id": f"evt-{int(time.time() * 1000)}-{random.randint(0, 999)}",
        "user_id": user_id,
        "user_tenure_days": random.randint(0, 2000),
        "action": random.choice(["click", "view", "purchase", "submit", "error"]),
        "amount": round(random.random() * 100, 2),
        "metadata": {"source": random.choice(["web", "mobile", "api"])},
        "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    }


def corrupt(ev):
    """Inject a realistic defect so the consumer's dead-letter path gets exercised."""
    defect = random.choice(["negative_amount", "unknown_action", "missing_user", "bad_ts"])
    if defect == "negative_amount":
        ev["amount"] = -ev["amount"] - 1
    elif defect == "unknown_action":
        ev["action"] = "refund??"
    elif defect == "missing_user":
        ev["user_id"] = None
    else:
        ev["ts"] = "not-a-timestamp"
    return ev


def main(topic='events', rate=1, bootstrap='localhost:9092', bad_rate=0.05):
    producer = KafkaProducer(bootstrap_servers=bootstrap,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    print(f"Producing to topic={topic} rate={rate}/s")
    try:
        while True:
            ev = random_event()
            if random.random() < bad_rate:
                ev = corrupt(ev)
            producer.send(topic, ev)
            print(f"-> {ev['event_id']} {ev['action']} user:{ev['user_id']}")
            time.sleep(1.0 / float(rate))
    except KeyboardInterrupt:
        print('Stopping producer')
    finally:
        producer.flush()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic', default='events')
    parser.add_argument('--rate', type=float, default=1.0, help='events per second')
    parser.add_argument('--bootstrap', default='localhost:9092')
    parser.add_argument('--bad-rate', type=float, default=0.05,
                        help='share of events with an injected defect')
    args = parser.parse_args()
    main(args.topic, args.rate, args.bootstrap, args.bad_rate)
