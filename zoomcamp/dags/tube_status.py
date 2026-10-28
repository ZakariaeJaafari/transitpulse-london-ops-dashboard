"""Orchestrate Tube line status bronze load (dlt) and line_status_snapshot mart (dbt)."""

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
    dag_id="tube_status",
    default_args=default_args,
    description="MinIO bronze → dlt → dbt line_status_snapshot",
    schedule=None,
    start_date=datetime(2026, 10, 27),
    catchup=False,
    tags=["zoomcamp", "tfl", "dbt"],
) as dag:
    load_tube_bronze = BashOperator(
        task_id="load_tube_bronze",
        bash_command=f"cd {ZOOMCAMP_ROOT} && uv run python ingest/tfl_line_status_dlt.py",
    )

    build_line_status_mart = BashOperator(
        task_id="build_line_status_mart",
        bash_command=(
            f"cd {ZOOMCAMP_ROOT}/transitPule_dbt && "
            "uv run dbt run --profiles-dir . --select line_status_snapshot --no-partial-parse"
        ),
    )

    load_tube_bronze >> build_line_status_mart
