"""Hourly precipitation — graded tile 2."""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st


def format_london_hour(ts: pd.Timestamp | object) -> str:
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return "—"
    return pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M")


def render(weather_df: pd.DataFrame) -> None:
    if weather_df.empty:
        st.warning("`marts.weather_hourly` has no rows on MotherDuck.")

    st.subheader("Hourly precipitation — London (local time)")
    if not weather_df.empty:
        weather_start = format_london_hour(weather_df["time"].min())
        weather_end = format_london_hour(weather_df["time"].max())
        weather_caption = (
            f"Open-Meteo hourly forecast from {weather_start} through "
            f"{weather_end} (Europe/London)."
        )
    else:
        weather_caption = "Open-Meteo hourly forecast (Europe/London)."
    st.caption(weather_caption)
    if weather_df.empty:
        return

    precip_chart = (
        alt.Chart(weather_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("time:T", title="Time (Europe/London)"),
            y=alt.Y("precipitation:Q", title="Precipitation (mm)"),
            tooltip=[
                alt.Tooltip("time:T", title="Hour"),
                alt.Tooltip("precipitation:Q", title="mm"),
            ],
        )
        .properties(height=420)
    )
    st.altair_chart(precip_chart, use_container_width=True)
