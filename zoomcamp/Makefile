SHELL := /bin/bash

.PHONY: up test dbt-build

up:
	docker compose up -d --build --wait

test:
	uv sync --all-groups
	uv run pytest

ifeq ($(OS),Windows_NT)
SHELL := bash.exe
else
SHELL := /bin/bash
endif

ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

dbt-build:
	set -a && . "$(ROOT).env" && set +a && export motherduck_token="$$MOTHERDUCK_TOKEN" && \
	cd "$(ROOT)transitPule_dbt" && \
	uv run dbt run --profiles-dir . --target motherduck --select +line_status_snapshot +weather_hourly +arrivals_by_line && \
	uv run dbt test --profiles-dir . --target motherduck --select line_status_snapshot weather_hourly arrivals_by_line
