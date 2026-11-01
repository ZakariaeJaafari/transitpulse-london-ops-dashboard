SHELL := /bin/bash

.PHONY: up test dbt-build

up:
	docker compose up -d --build --wait

test:
	uv sync --all-groups
	uv run pytest

dbt-build:
	set -a && source .env && set +a && export motherduck_token="$$MOTHERDUCK_TOKEN" && \
	cd transitPule_dbt && \
	uv run dbt run --profiles-dir . --target motherduck --select line_status_snapshot weather_hourly && \
	uv run dbt test --profiles-dir . --target motherduck --select line_status_snapshot weather_hourly
