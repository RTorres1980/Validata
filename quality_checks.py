#!/usr/bin/env python3
"""quality_checks.py
Runs Great Expectations (1.x API) data-quality checks against events_processed.

Usage:
  python quality_checks.py              # validate the Postgres table
  python quality_checks.py --csv f.csv  # validate an exported CSV instead

Exits non-zero when any expectation fails, so it can gate an Airflow task or CI job.
"""
import argparse
import os
import sys

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

from transform import VALID_ACTIONS

SUITE_NAME = "events_processed_suite"


def build_suite():
    suite = gx.ExpectationSuite(name=SUITE_NAME)
    suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=1))
    for col in ("event_id", "user_id", "action", "ts"):
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="event_id"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column="action", value_set=sorted(VALID_ACTIONS))
    )
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="amount", min_value=0))
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column="source", value_set=["web", "mobile", "api"], mostly=0.99)
    )
    return suite


def validate_dataframe(df):
    """Validate a DataFrame against the suite; returns the GE validation result."""
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas("events_source")
    asset = source.add_dataframe_asset("events_processed")
    batch_def = asset.add_batch_definition_whole_dataframe("full_table")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    suite = context.suites.add(build_suite())
    return batch.validate(suite)


def load_from_postgres():
    import psycopg2  # imported here so CSV mode works without a database driver

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "events_db"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
    )
    try:
        return pd.read_sql("SELECT * FROM public.events_processed", conn)
    finally:
        conn.close()


def summarize(result):
    for r in result.results:
        status = "PASS" if r.success else "FAIL"
        cfg = r.expectation_config
        col = cfg.kwargs.get("column", "(table)")
        print(f"[{status}] {cfg.type} on {col}")
    print("Overall:", "PASSED" if result.success else "FAILED")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", help="validate a CSV export instead of Postgres")
    args = parser.parse_args()
    df = pd.read_csv(args.csv) if args.csv else load_from_postgres()
    result = validate_dataframe(df)
    summarize(result)
    sys.exit(0 if result.success else 1)


if __name__ == "__main__":
    main()
