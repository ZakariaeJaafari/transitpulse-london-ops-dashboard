"""Fetch live TfL Tube line status and Open-Meteo London forecast into dated sample files."""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path

TFL_TUBE_STATUS_URL = "https://api.tfl.gov.uk/Line/Mode/tube/Status"
OPEN_METEO_LONDON_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=51.5074&longitude=-0.1278"
    "&hourly=temperature_2m,precipitation,wind_speed_10m"
    "&timezone=Europe%2FLondon"
)

SAMPLE_DATA_DIR = Path(__file__).resolve().parent.parent / "sample_data"


def _require_tfl_app_key() -> str:
    key = os.environ.get("TFL_APP_KEY", "").strip()
    if not key:
        print(
            "Error: TFL_APP_KEY is not set. Add it to zoomcamp/.env or export it in your shell.",
            file=sys.stderr,
        )
        sys.exit(1)
    return key


def _get(url: str) -> tuple[int, bytes]:
    request = urllib.request.Request(url, headers={"User-Agent": "TransitPulse/0.1"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def _count_tube_lines(body: bytes) -> int:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return 0
    if isinstance(payload, list):
        return len(payload)
    return 0


def _count_weather_hours(body: bytes) -> int:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return 0
    hourly = payload.get("hourly") if isinstance(payload, dict) else None
    if not isinstance(hourly, dict):
        return 0
    times = hourly.get("time")
    if isinstance(times, list):
        return len(times)
    return 0


def _require_http_ok(label: str, status: int) -> None:
    if status == 200:
        return
    print(f"Error: {label}: HTTP {status}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    app_key = _require_tfl_app_key()
    today = date.today().isoformat()

    tfl_url = f"{TFL_TUBE_STATUS_URL}?{urllib.parse.urlencode({'app_key': app_key})}"
    tfl_status, tfl_body = _get(tfl_url)
    _require_http_ok("TfL Tube status", tfl_status)

    weather_status, weather_body = _get(OPEN_METEO_LONDON_URL)
    _require_http_ok("Open-Meteo London", weather_status)

    SAMPLE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    tube_path = SAMPLE_DATA_DIR / f"tube_line_{today}.json"
    weather_path = SAMPLE_DATA_DIR / f"london_weather_{today}.json"

    tube_path.write_bytes(tfl_body)
    weather_path.write_bytes(weather_body)

    tube_lines = _count_tube_lines(tfl_body)
    weather_hours = _count_weather_hours(weather_body)

    print(f"TfL Tube status: HTTP {tfl_status}, {tube_lines} lines")
    print(f"Open-Meteo London: HTTP {weather_status}, {weather_hours} hourly rows")


if __name__ == "__main__":
    main()
