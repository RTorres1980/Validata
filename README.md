# Validata — Real-Time Data Quality Pipeline

![CI](https://github.com/RTorres1980/Validata/actions/workflows/ci.yml/badge.svg)

A streaming ETL pipeline that treats **data quality as a first-class step**, not an afterthought. Events flow through Kafka, are validated and flattened by a Python consumer, land in Postgres, and are then checked by a Great Expectations suite that can fail an Airflow run when the data goes bad.

The problem it addresses: in streaming pipelines, bad records rarely crash anything. They quietly land in the warehouse and corrupt downstream reports. Validata catches them at two layers and keeps every rejected record for inspection.

## Architecture

```
producer.py ──► Kafka "events" ──► consumer_etl.py ──► Postgres events_processed ──► quality_checks.py
  (simulated       topic              │  record-level                (warehouse)        (table-level
   events, ~5%                        │  validation                                    Great Expectations
   with defects)                      └──► Kafka "events.dlq"                          suite)
                                           (rejected events + reason)
                        dags/etl_dag.py orchestrates consumer → quality checks hourly
```

| Layer | What it checks | Where |
|---|---|---|
| Record-level | Missing IDs, unknown actions, negative or non-numeric amounts, unparseable timestamps | `transform.py` (called by the consumer) |
| Table-level | Row count, nulls, duplicate `event_id`s, value ranges, allowed sources | `quality_checks.py` (Great Expectations 1.x) |

Design choices worth noting:

- **Dead-letter queue instead of silent drops.** Invalid events go to `events.dlq` with the rejection reason, so they can be replayed or investigated.
- **At-least-once delivery.** Kafka offsets are committed only after a batch is written to Postgres; `ON CONFLICT DO NOTHING` makes replays idempotent.
- **Pure, testable core.** Validation logic lives in `transform.py` with no Kafka or database imports, so it's unit-tested in CI without any infrastructure.
- **Quality checks that gate.** `quality_checks.py` exits non-zero on failure, so the Airflow task fails and retries/alerts fire.

## Quick start

Requires Docker and Python 3.10+.

```bash
git clone https://github.com/RTorres1980/Validata.git
cd Validata
pip install -r requirements.txt

docker compose up -d                          # Kafka, Zookeeper, Postgres
python producer.py --topic events --rate 5    # terminal 1: generate events
python consumer_etl.py --topic events         # terminal 2: validate + load
python quality_checks.py                      # run the data-quality suite
```

Sample quality-check output:

```
[PASS] expect_table_row_count_to_be_between on (table)
[PASS] expect_column_values_to_not_be_null on event_id
[PASS] expect_column_values_to_be_unique on event_id
[PASS] expect_column_values_to_be_between on amount
...
Overall: PASSED
```

`quality_checks.py --csv export.csv` validates a CSV export instead of the live table.

## Tests

```bash
pip install -r requirements-dev.txt
pytest -q
```

Tests cover each rejection rule, the row transformation, and confirm the Great Expectations suite passes on clean data and flags duplicates and negative amounts.

## Project layout

```
producer.py           Simulated event stream with configurable defect rate (--bad-rate)
consumer_etl.py       Kafka → validation → Postgres, with dead-letter routing
transform.py          Pure validation/flattening logic
quality_checks.py     Great Expectations suite and runner
dags/etl_dag.py       Airflow DAG: consume → quality checks, hourly
docker-compose.yml    Local Kafka, Zookeeper, Postgres
tests/                Unit tests (run in GitHub Actions)
```

## Possible extensions

Schema registry (Avro/Protobuf) for contract enforcement, Prometheus metrics on rejection rates, and swapping Postgres for Snowflake or BigQuery.

## Author

Robert Torres · [LinkedIn](https://www.linkedin.com/in/robert-torres-394535381) · [GitHub](https://github.com/RTorres1980)
