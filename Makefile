.PHONY: up down producer consumer quality lint test

up:
	docker compose up -d

down:
	docker compose down

producer:
	python producer.py --topic events --rate 5

consumer:
	python consumer_etl.py --topic events

quality:
	python quality_checks.py

lint:
	flake8

test:
	pytest -q
