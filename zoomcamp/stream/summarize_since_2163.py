"""Summarize stream/output/arrival_updates_since_2163 by line_id (host-only sanity check)."""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

STREAM_DIR = Path(__file__).resolve().parent
SLICE_DIR = STREAM_DIR / "output" / "arrival_updates_since_2163"


def main() -> None:
    parts = sorted(p for p in SLICE_DIR.rglob("part-*") if p.is_file())
    if not parts:
        print(f"Error: no part-* file under {SLICE_DIR}", file=sys.stderr)
        sys.exit(1)
    if len(parts) > 1:
        print(
            "Error: more than one part-* file under "
            f"{SLICE_DIR}; a second Flink run would double-count.",
            file=sys.stderr,
        )
        sys.exit(1)

    counts: dict[str, int] = defaultdict(int)
    time_sums: dict[str, int] = defaultdict(int)
    for part in parts:
        for line in part.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            line_id = row["line_id"]
            counts[line_id] += 1
            time_sums[line_id] += int(row["time_to_station_sec"])

    total = 0
    for line_id in sorted(counts):
        n = counts[line_id]
        total += n
        avg_sec = time_sums[line_id] / n
        print(f"{line_id} {n} {avg_sec:.1f}")

    print(f"total {total}")


if __name__ == "__main__":
    main()
