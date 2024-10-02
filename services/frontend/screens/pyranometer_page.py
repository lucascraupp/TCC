import json

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

PLANTS_PARAM = json.load(open("services/resources/solar_plants.json"))
COLORS = {"Stow": "#fcb774", "Indisponível": "#FF6961", "Disponível": "#009de9"}


def get_data() -> None:
    st.session_state.gti = pd.read_parquet(
        PLANTS_PARAM[st.session_state.solar_plant]["gti_avg"]
    )
    st.session_state.teoric_irradiances = pd.read_parquet(
        PLANTS_PARAM[st.session_state.solar_plant]["teoric_irradiances_avg"]
    )
    st.session_state.ghi = pd.read_parquet(
        PLANTS_PARAM[st.session_state.solar_plant]["ghi_avg"]
    )
    st.session_state.clearsky = pd.read_parquet(
        PLANTS_PARAM[st.session_state.solar_plant]["clearsky"]
    )
    st.session_state.classification = pd.read_parquet(
        PLANTS_PARAM[st.session_state.solar_plant]["classification"]
    )

    st.session_state.min = st.session_state.gti.index.min()
    st.session_state.max = st.session_state.gti.index.max()
    st.session_state.begin = st.session_state.max - pd.DateOffset(weeks=1)


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

        get_data()


def header() -> None:
    cols = st.columns(2)
    with cols[0]:
        st.selectbox(
            "Selecione a usina",
            st.session_state.plants_list,
            key="solar_plant",
            on_change=get_data,
        )
    with cols[1]:
        min = st.session_state.min
        max = st.session_state.max
        begin = st.session_state.begin

        st.date_input(
            "Selecione um período",
            value=(begin, max),
            min_value=min,
            max_value=max,
            key="date_range",
        )

        if len(st.session_state.date_range) < 2:
            st.stop()


def generate_temporal_series(fig: go.Figure) -> None:
    gti = st.session_state.gti
    teoric_irradiances = st.session_state.teoric_irradiances
    ghi = st.session_state.ghi
    clearsky = st.session_state.clearsky

    begin, end = st.session_state.date_range

    gti = gti.loc[begin:end]
    teoric_irradiances = teoric_irradiances.loc[begin:end]
    ghi = ghi.loc[begin:end]
    clearsky = clearsky.loc[begin:end]

    df = pd.concat([gti, teoric_irradiances, ghi], axis=1)

    for sensor in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df[sensor],
                name=sensor,
                legendgroup=sensor,
            ),
            row=1,
            col=1,
        )

    # Adiciona GHI (ClearSky)
    fig.add_trace(
        go.Scatter(
            x=clearsky.index,
            y=clearsky[clearsky.columns[0]],
            name="GHI teórico (clearsky)",
            line=dict(color="gray", width=1.5, dash="dash"),
            legendgroup="GHI teórico",
        ),
        row=1,
        col=1,
    )


def generate_gantt_chart(fig: go.Figure) -> None:
    classification = st.session_state.classification
    begin, end = st.session_state.date_range

    gantt_list = []
    for date in pd.date_range(begin, end):
        day_class = classification.loc[classification.index.date == date.date()].copy()

        day_class["start"] = [
            date,
            day_class.index[1],
        ]
        day_class["end"] = [
            day_class.index[1] - pd.Timedelta(seconds=1),
            date + pd.Timedelta(hours=23, minutes=59, seconds=59),
        ]

        sensors_list = []
        for sensor in day_class.iloc[:, :-2]:
            df = pd.DataFrame()

            df["sensor"] = [sensor] * len(day_class)
            df["status"] = day_class[sensor].values
            df["start"] = day_class["start"].values
            df["end"] = day_class["end"].values
            df.index = [date] * len(day_class)

            sensors_list.append(df)

        gantt_list.append(pd.concat(sensors_list))

    gantt_chart = pd.concat(gantt_list)

    gantt_chart["description"] = gantt_chart.apply(
        lambda row: f"{row['sensor']} <br>{row['start']} - {row['end']} <br>{row['status']}",
        axis=1,
    )

    timeline = go.Figure()

    for _, row in gantt_chart.iterrows():
        timeline.add_trace(
            go.Scatter(
                x=[row["start"], row["end"]],
                y=[row["sensor"]] * 5,
                mode="lines",
                line=dict(
                    color=COLORS[row["status"]], width=25
                ),  # Customize line appearance (increased width)
                text=row["description"],
                hoverinfo="text",
                name=row["sensor"],  # Specify the name for the legend group
                legendgroup=row["sensor"],  # Set the legend group identifier
            )
        )

    # Cria um dicionário de mapeamento da ordem dos sensores no gantt_chart
    sensor_order = {
        sensor: i for i, sensor in enumerate(gantt_chart["sensor"].unique())
    }

    # Ordena timeline.data com base na ordem dos sensores no gantt_chart
    timeline.data = sorted(
        timeline.data, key=lambda trace: sensor_order[trace.name], reverse=True
    )

    for trace in timeline.data:
        fig.add_trace(trace, row=2, col=1)

    return fig


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

    generate_temporal_series(fig)
    generate_gantt_chart(fig)

    fig.update_xaxes(title_text="Timestamp", row=2, col=1)

    fig.update_yaxes(title_text="Irradiância (W/m²)", row=1, col=1)
    fig.update_yaxes(title_text="Sensores", row=2, col=1)

    fig.update_traces(showlegend=False, row=2, col=1)

    fig.update_layout(height=800)
    st.plotly_chart(fig, use_container_width=True)


def pyranometer_page() -> None:
    st.title("Classificação dos piranômetros")

    start_page()
    header()
    plot_graphs()
