import json
from pathlib import Path

from ingest.stoppoints import STOPPOINTS

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
STOPPOINTS_JSON = ZOOMCAMP_ROOT / "sample_data" / "stoppoints.json"


def test_stoppoints_json_matches_module() -> None:
    loaded = json.loads(STOPPOINTS_JSON.read_text(encoding="utf-8"))
    assert loaded == STOPPOINTS


def test_stoppoint_ids_unique_and_non_empty() -> None:
    stop_ids = [entry["stop_id"] for entry in STOPPOINTS]
    assert all(stop_ids)
    assert len(stop_ids) == len(set(stop_ids))
