from pathlib import Path

from lake.line_status_to_silver import OBS_TS, line_status_silver_rows

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
TUBE_BRONZE = ZOOMCAMP_ROOT / "sample_data" / "tube_line.json"


def test_line_status_silver_rows_from_sample_bytes() -> None:
    rows = line_status_silver_rows(TUBE_BRONZE.read_bytes())

    assert len(rows) == 11

    bakerloo = next(row for row in rows if row[0] == "bakerloo")
    line_id, severity, _description, disrupted, obs_ts = bakerloo
    assert line_id == "bakerloo"
    assert severity == 3
    assert disrupted is True
    assert obs_ts == OBS_TS
