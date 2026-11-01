import json
from pathlib import Path

from ingest.tfl_arrivals_producer import TOPIC, poll_arrivals, publish_arrivals

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
OXFORD_ARRIVALS = ZOOMCAMP_ROOT / "sample_data" / "arrivals_oxford_circus.json"
OBS_TS = "2026-11-01T17:13:00Z"
OXFORD_STOP_ID = "940GZZLUOXC"


def test_poll_arrivals_with_fake_fetch() -> None:
    oxford_predictions = json.loads(OXFORD_ARRIVALS.read_text(encoding="utf-8"))

    def fetch(stop_id: str) -> list[dict]:
        if stop_id == OXFORD_STOP_ID:
            return oxford_predictions
        return []

    messages = poll_arrivals(fetch, OBS_TS)

    assert TOPIC == "tfl.arrivals"
    assert len(messages) == 2
    assert all(msg["stop_id"] == OXFORD_STOP_ID for msg in messages)
    assert all(msg["obs_ts"] == OBS_TS for msg in messages)

    bakerloo = next(msg for msg in messages if msg["line_id"] == "bakerloo")
    victoria = next(msg for msg in messages if msg["line_id"] == "victoria")
    assert bakerloo["time_to_station_sec"] == 123
    assert victoria["time_to_station_sec"] == 65


def test_publish_arrivals_uses_stop_id_as_key() -> None:
    oxford_predictions = json.loads(OXFORD_ARRIVALS.read_text(encoding="utf-8"))

    def fetch(stop_id: str) -> list[dict]:
        if stop_id == OXFORD_STOP_ID:
            return oxford_predictions
        return []

    messages = poll_arrivals(fetch, OBS_TS)
    sent: list[tuple[str, str, dict]] = []

    def send(topic: str, key: str, value: dict) -> None:
        sent.append((topic, key, value))

    count = publish_arrivals(messages, send)

    assert count == 2
    assert len(sent) == 2
    assert all(topic == "tfl.arrivals" for topic, _key, _value in sent)
    assert all(key == OXFORD_STOP_ID for _topic, key, _value in sent)
    assert [value for _topic, _key, value in sent] == messages
