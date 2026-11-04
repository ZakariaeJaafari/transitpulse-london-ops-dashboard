"""Mean predicted seconds by line — optional full-width section."""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from labels import line_label


def render(arrivals_df: pd.DataFrame, arrival_updates_df: pd.DataFrame) -> None:
    if arrivals_df.empty:
        st.warning("`marts.arrivals_by_line` has no rows on MotherDuck.")

    st.subheader("Predicted seconds by line for the 2026-11-01 sample")
    st.caption(
        "Mean of TfL timeToStation predictions (not timetable delay). "
        "n is the number of distinct predictions for that line."
    )
    if arrivals_df.empty:
        return

    plot_arrivals = arrivals_df.copy()
    plot_arrivals["line"] = plot_arrivals["line_id"].map(line_label)

    arrivals_chart = (
        alt.Chart(plot_arrivals)
        .mark_bar()
        .encode(
            y=alt.Y("line:N", sort="-x", title="Tube line"),
            x=alt.X(
                "avg_predicted_seconds:Q",
                title="Mean predicted seconds (timeToStation)",
            ),
            tooltip=[
                alt.Tooltip("line", title="Line"),
                alt.Tooltip("avg_predicted_seconds:Q", title="Mean seconds"),
                alt.Tooltip("n:Q", title="Prediction count (n)"),
                alt.Tooltip("obs_date:T", title="Observation date"),
            ],
        )
        .properties(height=max(180, 48 * len(plot_arrivals)))
    )
    st.altair_chart(arrivals_chart, use_container_width=True)

    if arrival_updates_df.empty:
        st.warning("`marts.arrival_updates` has no rows on MotherDuck.")
        return

    st.caption(
        "Each row is one arrival prediction. "
        "time_to_station_sec is TfL timeToStation, not timetable delay."
    )
    table_df = arrival_updates_df.copy()
    table_df["line"] = table_df["line_id"].map(line_label)
    st.dataframe(
        table_df[
            [
                "line",
                "stop_id",
                "vehicle_id",
                "towards",
                "time_to_station_sec",
                "expected_arrival",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )
