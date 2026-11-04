"""One loader module per mart; each exposes load()."""

from __future__ import annotations

from data.arrival_updates import load as load_arrival_updates
from data.arrivals_by_line import load as load_arrivals_by_line
from data.line_status import load as load_line_status
from data.weather import load as load_weather

__all__ = [
    "load_arrival_updates",
    "load_arrivals_by_line",
    "load_line_status",
    "load_weather",
]
