"""TransitPulse Zoomcamp dashboard — read-only queries against local dbt marts."""

from __future__ import annotations

from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "transitpulse.duckdb"

LINE_LABELS: dict[str, str] = {
    "bakerloo": "Bakerloo",
    "central": "Central",
    "circle": "Circle",
    "district": "District",
    "hammersmith-city": "Hammersmith & City",
    "jubilee": "Jubilee",
    "metropolitan": "Metropolitan",
    "northern": "Northern",
    "piccadilly": "Piccadilly",
    "victoria": "Victoria",
    "waterloo-city": "Waterloo & City",
}


@st.cache_data(show_spinner="Loading marts…")
def load_marts(db_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    conn = duckdb.connect(db_path, read_only=True)
    try:
        lines = conn.execute(
            """
            select
                line_id,
                status_severity,
                status_description,
                disrupted,
                status_date
            from marts.line_status_snapshot
            order by status_severity asc, line_id
            """
        ).df()
        weather = conn.execute(
            """
            select time, precipitation
            from marts.weather_hourly
            order by time
            """
        ).df()
    finally:
        conn.close()
    return lines, weather


def line_label(line_id: str) -> str:
    return LINE_LABELS.get(line_id, line_id.replace("-", " ").title())


def format_london_hour(ts: pd.Timestamp | object) -> str:
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return "—"
    return pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M")


# TfL Good Service = 10; lower codes are worse — longer bars = worse service.
def disruption_bar_length(status_severity: int) -> int:
    return 11 - int(status_severity)


def main() -> None:
    st.set_page_config(page_title="TransitPulse", layout="wide")
    st.title("TransitPulse London")

    if not DB_PATH.is_file():
        st.error(
            f"Warehouse not found at `{DB_PATH}`. "
            "Run dlt ingest and `dbt run` in `transitPule_dbt/` first."
        )
        st.stop()

    lines_df, weather_df = load_marts(str(DB_PATH))

    if lines_df.empty:
        st.warning("`marts.line_status_snapshot` has no rows.")
    if weather_df.empty:
        st.warning("`marts.weather_hourly` has no rows.")

    status_date = lines_df["status_date"].iloc[0] if not lines_df.empty else None
    snapshot_title = (
        f"Tube line status — current snapshot ({status_date})"
        if status_date is not None
        else "Tube line status — current snapshot"
    )

    col_status, col_weather = st.columns(2)

    with col_status:
        st.subheader(snapshot_title)
        st.caption(
            "One row per Tube line for the latest dlt load date in bronze "
            "(not a rolling 7-day window). Longer bars mean worse service."
        )
        if not lines_df.empty:
            plot_lines = lines_df.copy()
            plot_lines["line"] = plot_lines["line_id"].map(line_label)
            plot_lines["status"] = plot_lines["status_description"]
            plot_lines["bar_length"] = plot_lines["status_severity"].map(
                disruption_bar_length
            )

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

    with col_weather:
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
        if not weather_df.empty:
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

    st.divider()
    st.caption(
        "Data provided by Transport for London and Open-Meteo. "
        "This is not an official TfL product."
    )


if __name__ == "__main__":
    main()
