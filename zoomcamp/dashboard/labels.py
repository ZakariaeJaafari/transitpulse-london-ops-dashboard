"""Shared TfL line display labels for dashboard charts."""

from __future__ import annotations

LINE_LABELS: dict[str, str] = {
    "bakerloo": "Bakerloo",
    "central": "Central",
    "circle": "Circle",
    "district": "District",
    "hammersmith-city": "Hammersmith & City",
    "jubilee": "Jubilee",
    "metropolitan": "Metropolitan",
    "northern": "Northern",
    "piccadilly": "Piccadilly",
    "victoria": "Victoria",
    "waterloo-city": "Waterloo & City",
}


def line_label(line_id: str) -> str:
    return LINE_LABELS.get(line_id, line_id.replace("-", " ").title())
