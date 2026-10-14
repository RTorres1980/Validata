"""Unit tests for the validation/transform logic and the data-quality suite."""
import os
import sys
from datetime import datetime

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transform import to_row, validate_event  # noqa: E402


def good_event(**overrides):
    ev = {
        "event_id": "evt-1",
        "user_id": 42,
        "user_tenure_days": 300,
        "action": "purchase",
        "amount": 19.99,
        "metadata": {"source": "web"},
        "ts": "2026-01-15T12:30:00Z",
    }
    ev.update(overrides)
    return ev


def test_valid_event_passes():
    assert validate_event(good_event()) == (True, None)


@pytest.mark.parametrize(
    "overrides, reason",
    [
        ({"event_id": ""}, "missing_ids"),
        ({"user_id": None}, "missing_ids"),
        ({"action": "refund??"}, "unknown_action"),
        ({"amount": -5}, "negative_amount"),
        ({"amount": "abc"}, "bad_amount"),
        ({"ts": "not-a-timestamp"}, "bad_timestamp"),
    ],
)
def test_invalid_events_are_rejected_with_reason(overrides, reason):
    assert validate_event(good_event(**overrides)) == (False, reason)


def test_non_dict_is_rejected():
    assert validate_event("garbage") == (False, "not_an_object")


def test_user_id_zero_is_not_treated_as_missing():
    assert validate_event(good_event(user_id=0))[0]


def test_to_row_flattens_event():
    row = to_row(good_event())
    assert row == ("evt-1", 42, 300, "purchase", 19.99, "web", datetime(2026, 1, 15, 12, 30))


def test_to_row_handles_missing_metadata():
    assert to_row(good_event(metadata=None))[5] is None


def _frame(rows):
    return pd.DataFrame([to_row(r) for r in rows], columns=[
        "event_id", "user_id", "user_tenure_days", "action", "amount", "source", "ts"])


def test_quality_suite_passes_on_clean_data():
    pytest.importorskip("great_expectations")
    from quality_checks import validate_dataframe

    df = _frame([good_event(event_id=f"evt-{i}") for i in range(20)])
    assert validate_dataframe(df).success


def test_quality_suite_catches_duplicates_and_negative_amounts():
    pytest.importorskip("great_expectations")
    from quality_checks import validate_dataframe

    df = _frame([good_event(event_id="dup"), good_event(event_id="dup")])
    df.loc[1, "amount"] = -10
    result = validate_dataframe(df)
    assert not result.success
    failed = {r.expectation_config.type for r in result.results if not r.success}
    assert "expect_column_values_to_be_unique" in failed
    assert "expect_column_values_to_be_between" in failed
