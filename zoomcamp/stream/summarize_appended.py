"""Summarize stream/output/arrival_updates_appended.jsonl by obs_ts and line_id (host-only)."""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

STREAM_DIR = Path(__file__).resolve().parent
JSONL_PATH = STREAM_DIR / "output" / "arrival_updates_appended.jsonl"


def main() -> None:
    if not JSONL_PATH.is_file():
        print(f"Error: missing {JSONL_PATH}", file=sys.stderr)
        sys.exit(1)

    by_obs: dict[str, tuple[dict[str, int], dict[str, int]]] = defaultdict(
        lambda: (defaultdict(int), defaultdict(int))
    )
    all_line_ids: set[str] = set()
    rows_read = 0
    for line in JSONL_PATH.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        obs_ts = row["obs_ts"]
        line_id = row["line_id"]
        all_line_ids.add(line_id)
        counts, time_sums = by_obs[obs_ts]
        counts[line_id] += 1
        time_sums[line_id] += int(row["time_to_station_sec"])
        rows_read += 1

    if rows_read == 0:
        print(f"Error: no rows in {JSONL_PATH}", file=sys.stderr)
        sys.exit(1)

    line_ids = sorted(all_line_ids)
    grand_total = 0
    for obs_ts in sorted(by_obs):
        counts, time_sums = by_obs[obs_ts]
        print(obs_ts)
        subtotal = 0
        for line_id in line_ids:
            n = counts.get(line_id, 0)
            if n == 0:
                print(f"{line_id} 0")
            else:
                subtotal += n
                avg_sec = time_sums[line_id] / n
                print(f"{line_id} {n} {avg_sec:.1f}")
        print(f"subtotal {subtotal}")
        grand_total += subtotal

    print(f"total {grand_total}")


if __name__ == "__main__":
    main()
