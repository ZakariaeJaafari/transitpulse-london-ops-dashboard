from pathlib import Path

from ingest.line_status_proxy import iter_line_status_rows

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
TUBE_SNAPSHOT = ZOOMCAMP_ROOT / "sample_data" / "tube_line_2026-10-27.json"


def test_iter_line_status_rows_tube_snapshot() -> None:
    rows = iter_line_status_rows(TUBE_SNAPSHOT)

    assert len(rows) == 11

    bakerloo = next(row for row in rows if row[0] == "bakerloo")
    line_id, severity, _description, disrupted = bakerloo
    assert line_id == "bakerloo"
    assert severity == 3
    assert disrupted is True
