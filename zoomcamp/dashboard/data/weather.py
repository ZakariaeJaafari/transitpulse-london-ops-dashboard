"""Loader for marts.weather_hourly."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from data.connection import query_df

SQL = """
select time, precipitation
from marts.weather_hourly
order by time
"""


@st.cache_data(show_spinner="Loading weather…")
def load() -> pd.DataFrame:
    return query_df(SQL)
