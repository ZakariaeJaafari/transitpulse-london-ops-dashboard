"""Orchestrate Open-Meteo weather bronze load (dlt) and weather_hourly mart (dbt)."""

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
    dag_id="weather_hourly",
    default_args=default_args,
    description="Sample weather JSON → dlt → dbt weather_hourly",
    schedule=None,
    start_date=datetime(2026, 10, 27),
    catchup=False,
    tags=["zoomcamp", "weather", "dbt"],
) as dag:
    load_weather_bronze = BashOperator(
        task_id="load_weather_bronze",
        bash_command=f"cd {ZOOMCAMP_ROOT} && uv run python ingest/weather_hourly_dlt.py",
    )

    build_weather_mart = BashOperator(
        task_id="build_weather_mart",
        bash_command=(
            f"cd {ZOOMCAMP_ROOT}/transitPule_dbt && "
            'export motherduck_token="$MOTHERDUCK_TOKEN" && '
            "uv run dbt run --profiles-dir . --target motherduck "
            "--select weather_hourly --no-partial-parse"
        ),
    )

    load_weather_bronze >> build_weather_mart
