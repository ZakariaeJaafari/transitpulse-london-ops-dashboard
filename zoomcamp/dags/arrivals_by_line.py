"""Orchestrate arrivals-by-line bronze load (dlt) and arrivals_by_line mart (dbt)."""

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
    description="MinIO silver Parquet → dlt → dbt arrivals_by_line",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["zoomcamp", "tfl", "dbt"],
) as dag:
    load_arrivals_bronze = BashOperator(
        task_id="load_arrivals_bronze",
        bash_command=(
            f"cd {ZOOMCAMP_ROOT} && "
            'export motherduck_token="$MOTHERDUCK_TOKEN" && '
            "uv run python ingest/arrivals_by_line_dlt.py"
        ),
    )

    build_arrivals_mart = BashOperator(
        task_id="build_arrivals_mart",
        bash_command=(
            f"cd {ZOOMCAMP_ROOT}/transitPule_dbt && "
            'export motherduck_token="$MOTHERDUCK_TOKEN" && '
            "uv run dbt run --profiles-dir . --target motherduck "
            "--select +arrivals_by_line --no-partial-parse"
        ),
    )

    load_arrivals_bronze >> build_arrivals_mart
