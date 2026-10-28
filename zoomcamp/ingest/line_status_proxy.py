"""Derive one current-status row per Tube line from a TfL line-status snapshot."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

GOOD_SERVICE_SEVERITY = 10

TUBE_LINE_SNAPSHOT = (
    Path(__file__).resolve().parent.parent / "sample_data" / "tube_line_2026-10-27.json"
)


def _is_current_status(status: dict[str, Any]) -> bool:
    periods = status.get("validityPeriods") or []
    return any(period.get("isNow") is True for period in periods)


def _pick_current_status(line: dict[str, Any]) -> dict[str, Any] | None:
    statuses = line.get("lineStatuses") or []
    if not statuses:
        return None

    current = [status for status in statuses if _is_current_status(status)]
    pool = current if current else statuses
    return min(pool, key=lambda status: status["statusSeverity"])


def line_status_row(line: dict[str, Any]) -> tuple[str, int | None, str, bool]:
    line_id = line["id"]
    status = _pick_current_status(line)
    if status is None:
        return line_id, None, "", False

    severity = int(status["statusSeverity"])
    description = status.get("statusSeverityDescription") or ""
    disrupted = severity != GOOD_SERVICE_SEVERITY
    return line_id, severity, description, disrupted


def iter_line_status_rows(
    snapshot_path: Path = TUBE_LINE_SNAPSHOT,
) -> list[tuple[str, int | None, str, bool]]:
    lines = json.loads(snapshot_path.read_text(encoding="utf-8"))
    return [line_status_row(line) for line in lines]


def main() -> None:
    for line_id, severity, description, disrupted in iter_line_status_rows():
        severity_text = "" if severity is None else str(severity)
        disrupted_text = "yes" if disrupted else "no"
        print(f"{line_id}, {severity_text}, {description}, {disrupted_text}")


if __name__ == "__main__":
    main()
