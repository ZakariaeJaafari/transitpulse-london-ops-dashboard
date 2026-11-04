"""TransitPulse Zoomcamp dashboard — layout composer for MotherDuck dbt marts."""

from __future__ import annotations

import streamlit as st

from charts import arrivals, line_status, weather
from data import (
    load_arrival_updates,
    load_arrivals_by_line,
    load_line_status,
    load_weather,
)


def main() -> None:
    st.set_page_config(page_title="TransitPulse", layout="wide")
    st.title("TransitPulse London")

    lines_df = load_line_status()
    weather_df = load_weather()
    arrivals_df = load_arrivals_by_line()
    arrival_updates_df = load_arrival_updates()

    col_status, col_weather = st.columns(2)
    with col_status:
        line_status.render(lines_df)
    with col_weather:
        weather.render(weather_df)

    arrivals.render(arrivals_df, arrival_updates_df)

    st.divider()
    st.caption(
        "Data provided by Transport for London and Open-Meteo. "
        "This is not an official TfL product."
    )


if __name__ == "__main__":
    main()
