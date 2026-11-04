"""Shared MotherDuck connection for dashboard loaders."""

from __future__ import annotations

import duckdb
import pandas as pd

MOTHERDUCK_DB = "md:transitpulse"


def query_df(sql: str) -> pd.DataFrame:
    conn = duckdb.connect(MOTHERDUCK_DB)
    try:
        return conn.execute(sql).df()
    finally:
        conn.close()
