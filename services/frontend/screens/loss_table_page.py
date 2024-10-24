import json
import math

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

PLANTS_PARAM = json.load(open("resources/solar_plants.json"))


def get_angle() -> None:
    angle = st.session_state.angle
    st.session_state.angle_table = st.session_state.loss_table[
        st.session_state.loss_table["Angulação (°)"] == angle
    ].reset_index(drop=True)


def get_data() -> None:
    solar_plant = st.session_state.solar_plant

    st.session_state.loss_table = pd.read_parquet(
        PLANTS_PARAM[solar_plant]["datawarehouse"]["loss_table"]
    )

    st.session_state.angle_list = st.session_state.loss_table["Angulação (°)"].unique()

    get_angle()


def start_page() -> None:
    def generate_keys(defaults: dict):
        # Inicia estado da sessão
        for key in defaults:
            if key not in st.session_state:
                st.session_state[key] = defaults[key]

    defaults = {
        "page": "loss_table_page",
        "first_start": True,
    }

    generate_keys(defaults)

    if st.session_state.page != "loss_table_page":
        st.session_state.clear()
        generate_keys(defaults)

    if st.session_state.first_start:
        st.session_state.plants_list = list(PLANTS_PARAM.keys())
        st.session_state.solar_plant = st.session_state.plants_list[0]
        st.session_state.angle = 0

        st.session_state.first_start = False

        get_data()


def header() -> None:
    col = st.columns(2)

    with col[0]:
        st.selectbox(
            "Selecione a usina",
            st.session_state.plants_list,
            key="solar_plant",
            on_change=get_data,
        )
    with col[1]:
        st.selectbox(
            "Selecione o ângulo",
            st.session_state.angle_list,
            key="angle",
            on_change=get_angle,
        )


def plot_loss_per_angle_bar() -> None:
    st.title("Comportamento das perdas para diferentes CSI's")

    loss_table = st.session_state.loss_table

    csi_upper_90 = loss_table[loss_table["CSI"] > 0.9]
    csi_lower_40 = loss_table[loss_table["CSI"] < 0.4]

    upper_csi = loss_table[
        loss_table["Data"] == np.random.choice(csi_upper_90["Data"].unique())
    ]

    lower_csi = loss_table[
        loss_table["Data"] == np.random.choice(csi_lower_40["Data"].unique())
    ]

    table_list = [upper_csi, lower_csi]

    col = st.columns(2)
    i = 0
    for table in table_list:
        with col[i]:
            fig = go.Figure(
                data=[
                    go.Bar(
                        x=table["Angulação (°)"],
                        y=table["Perda (%)"],
                        marker_color=[
                            "#FF6961" if value > 0 else "#55CBCD"
                            for value in table["Perda (%)"]
                        ],
                        # Adiciona a legenda ao passar o mouse
                        hovertemplate=(
                            "CSI: %{customdata[0]}<br>"
                            "Angulação: %{x}°<br>"
                            "Perda: %{y}%<extra></extra>"
                        ),
                        # Passa os dados customizados
                        customdata=np.stack([table["CSI"]], axis=-1),
                    )
                ]
            )

            fig.update_layout(
                xaxis_title="Angulação (°)",
                yaxis_title="Perda (%)",
                title=f"Perda por angulação ({table['Data'].values[0].strftime('%d/%m/%Y')})",
            )

            st.plotly_chart(fig, use_container_width=True)

        i += 1 if i == 0 else -1


def calculate_interval(loss_table: pd.DataFrame, step: float) -> pd.DataFrame:
    csi_range = np.arange(0, 1 - step, step)

    table_list = [
        loss_table[
            (loss_table["CSI"] >= round(csi, 2))
            & (loss_table["CSI"] < round(csi + step, 2))
        ]
        for csi in csi_range
    ]

    # Tratamento especial para o último intervalo
    last_interval = loss_table[
        (loss_table["CSI"] >= 1 - step) & (loss_table["CSI"] <= 1)
    ]
    table_list.append(last_interval)

    return pd.DataFrame(
        {
            "Intervalo CSI": [f"[{csi:.2f}, {csi + step:.2f})" for csi in csi_range]
            + [f"[{1 - step:.2f}, 1.00]"],
            "Número de dias": [len(table) for table in table_list],
            "Perda mínima (%)": [table["Perda (%)"].min() for table in table_list],
            "Perda média (%)": [
                round(table["Perda (%)"].mean(), 2) for table in table_list
            ],
            "Perda máxima (%)": [table["Perda (%)"].max() for table in table_list],
        }
    )


def calculcate_quantile(loss_table: pd.DataFrame, quantile: float) -> pd.DataFrame:
    table_by_csi = loss_table.sort_values(by="CSI").reset_index(drop=True)

    length = math.ceil(len(table_by_csi) * quantile)

    quant_list = []
    begin = 0
    for end in range(length, len(table_by_csi) + length, length):
        table = table_by_csi.loc[begin : end - 1]

        quant_list.append(
            pd.DataFrame(
                {
                    "Intervalo CSI": [
                        f"[{table["CSI"].min():.2f}, {table["CSI"].max():.2f}]"
                    ],
                    "Número de dias": len(table),
                    "Perda mínima (%)": table["Perda (%)"].min(),
                    "Perda média (%)": round(table["Perda (%)"].mean(), 2),
                    "Perda máxima (%)": table["Perda (%)"].max(),
                }
            )
        )

        begin = end

    return pd.concat(quant_list, ignore_index=True)


def plot_loss_per_csi_scatter() -> None:
    st.title("Comportamento das perdas para diferentes intervalos de CSI")

    loss_table = st.session_state.angle_table

    step = 0.1
    quantile = 0.1

    interval_table = calculate_interval(loss_table, step)
    quantile_table = calculcate_quantile(loss_table, quantile)

    fig = go.Figure(
        data=[
            go.Scatter(
                x=interval_table["Intervalo CSI"],
                y=interval_table["Perda média (%)"],
                mode="markers",
                hovertemplate=(
                    "Intervalo CSI: %{x}<br>"
                    "Número de dias: %{customdata[0]}<br>"
                    "Perda mínima: %{customdata[1]}%<br>"
                    "Perda média: %{y}%<extra></extra><br>"
                    "Perda máxima: %{customdata[2]}%<br>"
                ),
                customdata=np.hstack(
                    [
                        interval_table[
                            ["Número de dias", "Perda mínima (%)", "Perda máxima (%)"]
                        ]
                    ]
                ),
            )
        ]
    )

    fig2 = go.Figure(
        data=[
            go.Scatter(
                x=quantile_table["Intervalo CSI"],
                y=quantile_table["Perda média (%)"],
                mode="markers",
                hovertemplate=(
                    "Intervalo CSI: %{x}<br>"
                    "Número de dias: %{customdata[0]}<br>"
                    "Perda mínima: %{customdata[1]}%<br>"
                    "Perda média: %{y}%<extra></extra><br>"
                    "Perda máxima: %{customdata[2]}%<br>"
                ),
                customdata=np.hstack(
                    [
                        quantile_table[
                            ["Número de dias", "Perda mínima (%)", "Perda máxima (%)"]
                        ]
                    ]
                ),
            )
        ]
    )

    fig.update_layout(
        xaxis_title="Intervalo CSI",
        yaxis_title="Perda média (%)",
        title=f"Perda por intervalo de {step}",
    )

    fig2.update_layout(
        xaxis_title="Intervalo CSI",
        yaxis_title="Perda média (%)",
        title=f"Perda por quantil de {quantile}",
    )

    col = st.columns(2)

    with col[0]:
        st.plotly_chart(fig, use_container_width=True)
    with col[1]:
        st.plotly_chart(fig2, use_container_width=True)


def loss_table_page() -> None:
    start_page()
    header()
    plot_loss_per_angle_bar()
    plot_loss_per_csi_scatter()
