"""Load Open-Meteo hourly weather JSON into DuckDB bronze with dlt."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import dlt

SAMPLE_PATH = (
    Path(__file__).resolve().parent.parent
    / "sample_data"
    / "london_weather_2026-10-27.json"
)
MOTHERDUCK_DB = "md:transitpulse"

RESOURCE_NAME = "weather_hourly"


def _iter_hourly_records(payload: dict) -> list[dict]:
    hourly = payload["hourly"]
    times = hourly["time"]
    temperatures = hourly["temperature_2m"]
    precipitations = hourly["precipitation"]
    wind_speeds = hourly["wind_speed_10m"]

    return [
        {
            "time": times[i],
            "temperature_2m": temperatures[i],
            "precipitation": precipitations[i],
            "wind_speed_10m": wind_speeds[i],
        }
        for i in range(len(times))
    ]


@dlt.resource(name=RESOURCE_NAME, write_disposition="replace")
def weather_hourly():
    payload = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
    for record in _iter_hourly_records(payload):
        yield record


def _print_post_load_report() -> None:
    con = duckdb.connect(MOTHERDUCK_DB)
    try:
        row_count = con.execute(
            f"SELECT count(*) FROM bronze.{RESOURCE_NAME}"
        ).fetchone()[0]
        sample = con.execute(
            f"""
            SELECT time, temperature_2m, precipitation, wind_speed_10m
            FROM bronze.{RESOURCE_NAME}
            ORDER BY time
            LIMIT 3
            """
        ).fetchall()
        print(f"{RESOURCE_NAME} rows: {row_count}")
        print("first hours:", sample)
    finally:
        con.close()


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="weather_hourly",
        destination=dlt.destinations.motherduck(credentials=MOTHERDUCK_DB),
        dataset_name="bronze",
    )
    load_info = pipeline.run(weather_hourly())
    print(load_info)
    _print_post_load_report()


if __name__ == "__main__":
    main()
