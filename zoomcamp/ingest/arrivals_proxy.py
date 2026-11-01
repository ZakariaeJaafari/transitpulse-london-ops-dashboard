"""Normalize TfL StopPoint arrival predictions for curated stops."""

from __future__ import annotations

from typing import Any

from ingest.stoppoints import STOPPOINTS

ALLOWED_STOP_IDS = {entry["stop_id"] for entry in STOPPOINTS}


def arrival_rows(predictions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for prediction in predictions:
        if prediction.get("modeName") != "tube":
            continue
        naptan_id = prediction.get("naptanId")
        if naptan_id not in ALLOWED_STOP_IDS:
            continue
        rows.append(
            {
                "stop_id": naptan_id,
                "line_id": prediction["lineId"],
                "vehicle_id": prediction["vehicleId"],
                "expected_arrival": prediction["expectedArrival"],
                "time_to_station_sec": int(prediction["timeToStation"]),
                "towards": prediction["towards"],
            }
        )
    return rows
