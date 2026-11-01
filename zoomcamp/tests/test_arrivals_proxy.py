import json
from pathlib import Path

from ingest.arrivals_proxy import arrival_rows

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
OXFORD_ARRIVALS = ZOOMCAMP_ROOT / "sample_data" / "arrivals_oxford_circus.json"


def test_arrival_rows_from_oxford_circus_sample() -> None:
    predictions = json.loads(OXFORD_ARRIVALS.read_text(encoding="utf-8"))
    rows = arrival_rows(predictions)

    assert len(rows) == 2

    bakerloo = next(row for row in rows if row["line_id"] == "bakerloo")
    victoria = next(row for row in rows if row["line_id"] == "victoria")

    assert bakerloo["time_to_station_sec"] == 123
    assert victoria["time_to_station_sec"] == 65
    assert bakerloo["stop_id"] == "940GZZLUOXC"
    assert victoria["stop_id"] == "940GZZLUOXC"


def test_arrival_rows_drops_non_tube_and_unknown_stop() -> None:
    predictions = json.loads(OXFORD_ARRIVALS.read_text(encoding="utf-8"))
    extra = list(predictions)
    extra.append(
        {
            "naptanId": "940GZZLUOXC",
            "lineId": "159",
            "vehicleId": "bus-1",
            "expectedArrival": "2026-11-01T17:20:00Z",
            "timeToStation": 300,
            "towards": "Somewhere",
            "modeName": "bus",
        }
    )
    extra.append(
        {
            "naptanId": "940GZZLUUNKNOWN",
            "lineId": "central",
            "vehicleId": "999",
            "expectedArrival": "2026-11-01T17:20:00Z",
            "timeToStation": 90,
            "towards": "Ealing Broadway",
            "modeName": "tube",
        }
    )

    rows = arrival_rows(extra)
    assert len(rows) == 2
