"""Loader for marts.arrivals_by_line."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from data.connection import query_df

SQL = """
select
    line_id,
    n,
    avg_predicted_seconds,
    obs_date
from marts.arrivals_by_line
order by line_id
"""


@st.cache_data(show_spinner="Loading arrivals by line…")
def load() -> pd.DataFrame:
    return query_df(SQL)
