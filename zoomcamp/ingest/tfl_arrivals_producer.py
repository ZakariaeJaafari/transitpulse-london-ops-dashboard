"""Build arrivals poll messages for curated StopPoints (pure function; no I/O here)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ingest.arrivals_proxy import arrival_rows
from ingest.stoppoints import STOPPOINTS

TOPIC = "tfl.arrivals"


def poll_arrivals(
    fetch: Callable[[str], list[dict[str, Any]]],
    obs_ts: str,
) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    for stop in STOPPOINTS:
        stop_id = stop["stop_id"]
        predictions = fetch(stop_id)
        for row in arrival_rows(predictions):
            messages.append({**row, "obs_ts": obs_ts})
    return messages


def publish_arrivals(
    messages: list[dict[str, Any]],
    send: Callable[[str, str, dict[str, Any]], None],
) -> int:
    for message in messages:
        send(TOPIC, message["stop_id"], message)
    return len(messages)
