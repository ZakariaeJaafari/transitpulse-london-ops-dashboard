"""Run poll_live_arrivals twice with a 20s gap (lesson: two live polls with follower open)."""

from __future__ import annotations

import sys
import time
from pathlib import Path

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
if str(ZOOMCAMP_ROOT) not in sys.path:
    sys.path.insert(0, str(ZOOMCAMP_ROOT))

from ingest import poll_live_arrivals


def main() -> None:
    poll_live_arrivals.main()
    time.sleep(20)
    poll_live_arrivals.main()


if __name__ == "__main__":
    main()
