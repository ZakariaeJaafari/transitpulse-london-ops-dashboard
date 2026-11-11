"""Single chain: Tube + weather bronze (dlt) then both marts (dbt)."""

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
    dag_id="transit_batch",
    default_args=default_args,
    description="Tube + weather: dlt bronze → MinIO silver → dbt marts (one chain)",
    schedule=None,
    start_date=datetime(2026, 10, 27),
    catchup=False,
    tags=["zoomcamp", "tfl", "weather", "dbt"],
) as dag:
    load_tube_bronze = BashOperator(
        task_id="load_tube_bronze",
        bash_command=(
            f"cd {ZOOMCAMP_ROOT} && "
            'export motherduck_token="$MOTHERDUCK_TOKEN" && '
            "uv run python ingest/tfl_line_status_dlt.py"
        ),
    )

    write_line_status_silver = BashOperator(
        task_id="write_line_status_silver",
        bash_command=(
            f"cd {ZOOMCAMP_ROOT} && "
            "uv run python lake/line_status_to_silver.py"
        ),
    )

    load_weather_bronze = BashOperator(
        task_id="load_weather_bronze",
        bash_command=(
            f"cd {ZOOMCAMP_ROOT} && "
            'export motherduck_token="$MOTHERDUCK_TOKEN" && '
            "uv run python ingest/weather_hourly_dlt.py"
        ),
    )

    build_line_status_mart = BashOperator(
        task_id="build_line_status_mart",
        bash_command=(
            f"cd {ZOOMCAMP_ROOT}/transitPule_dbt && "
            'export motherduck_token="$MOTHERDUCK_TOKEN" && '
            "uv run dbt build --profiles-dir . --target motherduck "
            "--select +line_status_snapshot --no-partial-parse"
        ),
    )

    build_weather_mart = BashOperator(
        task_id="build_weather_mart",
        bash_command=(
            f"cd {ZOOMCAMP_ROOT}/transitPule_dbt && "
            'export motherduck_token="$MOTHERDUCK_TOKEN" && '
            "uv run dbt build --profiles-dir . --target motherduck "
            "--select +weather_hourly --no-partial-parse"
        ),
    )

    (
        load_tube_bronze
        >> write_line_status_silver
        >> load_weather_bronze
        >> build_line_status_mart
        >> build_weather_mart
    )
