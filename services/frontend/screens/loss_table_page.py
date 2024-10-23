import json

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

PLANTS_PARAM = json.load(open("resources/solar_plants.json"))


def get_angle() -> None:
    angle = st.session_state.angle
    st.session_state.angle_table = st.session_state.loss_table[
        st.session_state.loss_table["Angulação (°)"] == angle
    ]


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


def plot_loss_per_csi_scatter() -> None:
    loss_table = st.session_state.angle_table

    step = 0.1

    csi_range = np.arange(0, 1 - step, step)

    table_list = [
        loss_table[(loss_table["CSI"] >= csi) & (loss_table["CSI"] < csi + 0.1)]
        for csi in csi_range
    ]

    # Tratamento especial para o último intervalo
    last_interval = loss_table[
        (loss_table["CSI"] >= 1 - step) & (loss_table["CSI"] <= 1.0)
    ]
    table_list.append(last_interval)

    csi_table = pd.DataFrame(
        {
            "Intervalo CSI": [
                f"[{csi:.2f}, {csi + step - 0.01:.2f}]" for csi in csi_range
            ]
            + [f"[{1 - step:.2f}, 1.00]"],
            "Número de Dias": [len(table) for table in table_list],
            "Perda Mínima (%)": [table["Perda (%)"].min() for table in table_list],
            "Perda Média (%)": [table["Perda (%)"].mean() for table in table_list],
            "Perda Máxima (%)": [table["Perda (%)"].max() for table in table_list],
        }
    )

    st.write(csi_table)


def loss_table_page() -> None:
    start_page()
    header()
    plot_loss_per_angle_bar()
    plot_loss_per_csi_scatter()
