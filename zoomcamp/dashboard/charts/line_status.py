"""Tube line status snapshot — graded tile 1."""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from labels import line_label


def disruption_bar_length(status_severity: int) -> int:
    # TfL Good Service = 10; lower codes are worse — longer bars = worse service.
    return 11 - int(status_severity)


def render(lines_df: pd.DataFrame) -> None:
    if lines_df.empty:
        st.warning("`marts.line_status_snapshot` has no rows on MotherDuck.")

    status_date = lines_df["status_date"].iloc[0] if not lines_df.empty else None
    snapshot_title = (
        f"Tube line status — current snapshot ({status_date})"
        if status_date is not None
        else "Tube line status — current snapshot"
    )

    st.subheader(snapshot_title)
    st.caption(
        "One row per Tube line for the latest dlt load date in bronze "
        "(not a rolling 7-day window). Longer bars mean worse service."
    )
    if lines_df.empty:
        return

    plot_lines = lines_df.copy()
    plot_lines["line"] = plot_lines["line_id"].map(line_label)
    plot_lines["status"] = plot_lines["status_description"]
    plot_lines["bar_length"] = plot_lines["status_severity"].map(disruption_bar_length)

    severity_chart = (
        alt.Chart(plot_lines)
        .mark_bar()
        .encode(
            y=alt.Y(
                "line:N",
                sort=alt.EncodingSortField(
                    field="status_severity",
                    order="ascending",
                ),
                title="Tube line",
            ),
            x=alt.X(
                "bar_length:Q",
                title="Service disruption (longer = worse)",
                scale=alt.Scale(domain=[0, 10]),
            ),
            color=alt.Color("status:N", title="Status label"),
            tooltip=[
                alt.Tooltip("line", title="Line"),
                alt.Tooltip("status", title="Status"),
                alt.Tooltip("status_severity", title="Severity code"),
            ],
        )
        .properties(height=420)
    )
    st.altair_chart(severity_chart, use_container_width=True)
