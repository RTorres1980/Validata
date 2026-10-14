"""transform.py
Pure validation and transformation logic shared by the consumer and the tests.
Kept free of Kafka/Postgres imports so it can be unit-tested in isolation.
"""
from datetime import datetime

VALID_ACTIONS = {"click", "view", "purchase", "submit", "error"}


def validate_event(rec):
    """Return (is_valid, reason). reason is None when the record is valid."""
    if not isinstance(rec, dict):
        return False, "not_an_object"
    if not rec.get("event_id") or rec.get("user_id") is None:
        return False, "missing_ids"
    if rec.get("action") not in VALID_ACTIONS:
        return False, "unknown_action"
    try:
        amount = float(rec.get("amount") or 0)
    except (TypeError, ValueError):
        return False, "bad_amount"
    if amount < 0:
        return False, "negative_amount"
    try:
        parse_ts(rec["ts"])
    except (KeyError, TypeError, ValueError):
        return False, "bad_timestamp"
    return True, None


def parse_ts(value):
    """Parse an ISO-8601 timestamp, accepting a trailing 'Z'."""
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)


def to_row(rec):
    """Flatten a validated event into a tuple matching events_processed."""
    return (
        rec["event_id"],
        int(rec["user_id"]),
        int(rec.get("user_tenure_days") or 0),
        rec["action"],
        float(rec.get("amount") or 0),
        (rec.get("metadata") or {}).get("source"),
        parse_ts(rec["ts"]),
    )
