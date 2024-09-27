import json
import sys

sys.path.append("services/")

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from db.src.classification_pyranometer import get_classification, get_clear_sky
from plotly.subplots import make_subplots

PLANTS_PARAM = json.load(open("services/resources/solar_plants.json"))


def read_data(type_data: str) -> pd.DataFrame:
    data = pd.read_parquet(PLANTS_PARAM[st.session_state.solar_plant][type_data])
    data = data.set_index("Timestamp")

    data = data.filter(like=st.session_state.park)

    data = data.apply(pd.to_numeric, errors="coerce")
    data = data.fillna(0)

    return data


def update_data() -> None:
    if st.session_state.date_range is None:
        date_range = (st.session_state.start_week, st.session_state.end_calendar)
    else:
        date_range = st.session_state.date_range

    st.session_state.classification = get_classification(
        st.session_state.solar_plant,
        st.session_state.park,
        date_range[0],
        date_range[1],
    )

    clearsky = pd.DataFrame()
    for date in date_range:
        clearsky = pd.concat(
            [clearsky, get_clear_sky(st.session_state.solar_plant, date)]
        )

    st.session_state.clearsky = clearsky


def generate_park_list() -> None:
    park_list = list(PLANTS_PARAM[st.session_state.solar_plant]["parks"].keys())

    st.session_state.park_list = park_list
    st.session_state.park = park_list[0]

    gti = read_data("gti")
    ghi = read_data("ghi")

    st.session_state.sensor_data = pd.concat([gti, ghi], axis=1)


def generate_dates() -> None:
    sensor_data = st.session_state.sensor_data

    start_date = sensor_data.index.min().date()
    end_date = sensor_data.index.max().date()

    start_week = end_date - pd.DateOffset(weeks=1)

    st.session_state.begin_calendar = start_date
    st.session_state.end_calendar = end_date
    st.session_state.start_week = start_week

    update_data()


def generate_header_params() -> None:
    generate_park_list()
    generate_dates()


def start_page() -> None:
    def generate_keys(defaults: dict):
        # Inicia estado da sessão
        for key in defaults:
            if key not in st.session_state:
                st.session_state[key] = defaults[key]

    defaults = {
        "page": "pyranometer_page",
        "first_start": True,
    }

    generate_keys(defaults)

    if st.session_state.page != "pyranometer_page":
        st.session_state.clear()
        generate_keys(defaults)

    if st.session_state.first_start:
        st.session_state.plants_list = list(PLANTS_PARAM.keys())
        st.session_state.solar_plant = st.session_state.plants_list[0]

        st.session_state.first_start = False

        generate_header_params()


def header() -> None:
    begin_calendar = st.session_state.begin_calendar
    end_calendar = st.session_state.end_calendar
    start_week = st.session_state.start_week

    cols = st.columns(3)

    with cols[0]:
        st.selectbox(
            "Selecione a usina",
            st.session_state.plants_list,
            key="solar_plant",
            on_change=generate_header_params,
        )

    with cols[1]:
        st.selectbox(
            "Selecione o parque",
            st.session_state.park_list,
            key="park",
            on_change=generate_dates,
        )

    with cols[2]:
        st.date_input(
            "Selecione um período",
            value=(start_week, end_calendar),
            min_value=begin_calendar,
            max_value=end_calendar,
            key="date_range",
            on_change=update_data,
        )

        if len(st.session_state.date_range) < 2:
            st.stop()


def generate_temporal_series(data: pd.DataFrame, fig: go.Figure) -> go.Figure:
    # Adiciona curvas de irradiância dos sensores
    for sensor in data.columns:
        fig.add_trace(
            go.Scatter(
                x=data.index,
                y=data[sensor],
                name=sensor,
                legendgroup=sensor,  # Set the legend group identifier
            ),
            row=1,
            col=1,
        )

    clearsky = st.session_state.clearsky

    # Adiciona GHI (ClearSky)
    fig.add_trace(
        go.Scatter(
            x=clearsky.index,
            y=clearsky,
            name="GHI teórico (clearsky)",
            line=dict(color="gray", width=1.5, dash="dash"),
            legendgroup="GHI teórico",
        ),
        row=1,
        col=1,
    )

    return fig

    # # fig.update_yaxes(title_text="Irradiância (W/m²)", row=1, col=1)
    # fig.update_layout(
    #     title="Série temporal dos piranômetros",
    #     xaxis_title="Timestamp",
    #     yaxis_title="Irradiância (W/m²)",
    # )
    # st.plotly_chart(fig, use_container_width=True)


def generate_gantt_chart(fig: go.Figure) -> go.Figure:
    return


def plot_graphs() -> None:
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        subplot_titles=(
            "Curvas de irradiância dos sensores",
            "Disponibilidade dos sensores",
        ),
    )
    st.write(type(fig))

    data = st.session_state.sensor_data
    date_range = st.session_state.date_range

    data = data.loc[date_range[0] : date_range[1]]

    # fig = generate_temporal_series(fig)


def pyranometer_page() -> None:
    st.title("Classificação dos piranômetros")

    start_page()
    header()
    plot_graphs()
