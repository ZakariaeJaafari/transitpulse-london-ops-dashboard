"""Orchestrate arrival_updates bronze load (dlt) and dbt build (marts + contract tests)."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

ZOOMCAMP_ROOT = "/opt/transitpulse/zoomcamp"

default_args = {
    "owner": "transitpulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="arrivals_by_line",
    default_args=default_args,
    description="MinIO silver arrival_updates JSON → dlt → dbt build (marts + contract tests)",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["zoomcamp", "tfl", "dbt"],
) as dag:
    load_arrival_updates = BashOperator(
        task_id="load_arrival_updates",
        bash_command=(
            f"cd {ZOOMCAMP_ROOT} && "
            'export motherduck_token="$MOTHERDUCK_TOKEN" && '
            "uv run python ingest/arrival_updates_dlt.py"
        ),
    )

    build_arrivals_mart = BashOperator(
        task_id="build_arrivals_mart",
        bash_command=(
            f"cd {ZOOMCAMP_ROOT}/transitPule_dbt && "
            'export motherduck_token="$MOTHERDUCK_TOKEN" && '
            "uv run dbt build --profiles-dir . --target motherduck "
            "--select +arrivals_by_line +arrival_updates arrivals_by_line_matches_updates "
            "--no-partial-parse"
        ),
    )

    load_arrival_updates >> build_arrivals_mart
