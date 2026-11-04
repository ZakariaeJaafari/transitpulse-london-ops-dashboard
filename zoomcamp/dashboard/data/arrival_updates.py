"""Loader for marts.arrival_updates."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from data.connection import query_df

SQL = """
select
    stop_id,
    line_id,
    vehicle_id,
    towards,
    time_to_station_sec,
    expected_arrival
from marts.arrival_updates
order by line_id, vehicle_id
"""


@st.cache_data(show_spinner="Loading arrival updates…")
def load() -> pd.DataFrame:
    return query_df(SQL)
