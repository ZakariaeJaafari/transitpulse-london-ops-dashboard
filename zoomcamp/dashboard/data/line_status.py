"""Loader for marts.line_status_snapshot."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from data.connection import query_df

SQL = """
select
    line_id,
    status_severity,
    status_description,
    disrupted,
    status_date
from marts.line_status_snapshot
order by status_severity asc, line_id
"""


@st.cache_data(show_spinner="Loading line status…")
def load() -> pd.DataFrame:
    return query_df(SQL)
