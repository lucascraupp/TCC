import json
import math

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

PLANTS_PARAM = json.load(open("resources/solar_plants.json"))


def get_data() -> None:
    solar_plant = st.session_state.solar_plant

    st.session_state.loss_table = pd.read_parquet(
        PLANTS_PARAM[solar_plant]["datawarehouse"]["loss_table"]
    )


def create_grouped_csi_scatter(data: pd.DataFrame, title: str, color: str) -> go.Figure:
    return go.Figure(
        data=[
            go.Scatter(
                x=data["Intervalo CSI"],
                y=data["Perda média (%)"],
                mode="markers + lines",
                marker_color=color,
                hovertemplate=(
                    "Intervalo CSI: %{x}<br>"
                    "Número de dias: %{customdata[0]}<br>"
                    "Perda mínima: %{customdata[1]}%<br>"
                    "Perda média: %{y}%<extra></extra><br>"
                    "Perda máxima: %{customdata[2]}%<br>"
                ),
                customdata=np.hstack(
                    [
                        data[
                            [
                                "Número de dias",
                                "Perda mínima (%)",
                                "Perda máxima (%)",
                            ]
                        ]
                    ]
                ),
            )
        ],
        layout=go.Layout(
            title=title,
            titlefont=dict(size=16),
            xaxis=dict(
                title="Intervalo CSI",
                title_font=dict(size=16),
                tickfont=dict(size=16),
                showgrid=True,
            ),
            yaxis=dict(
                title="Perda média (%)",
                title_font=dict(size=16),
                tickfont=dict(size=16),
            ),
        ),
    )


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


def start_page() -> None:
    def generate_keys(defaults: dict) -> None:
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

        st.session_state.first_start = False

        get_data()


def header() -> None:
    st.selectbox(
        "Selecione a usina",
        st.session_state.plants_list,
        key="solar_plant",
        on_change=get_data,
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
                ],
                layout=go.Layout(
                    title=f"Perda por angulação ({table['Data'].values[0].strftime('%d/%m/%Y')})",
                    titlefont=dict(size=16),
                    xaxis=dict(
                        title="Angulação (°)",
                        title_font=dict(size=16),
                        tickfont=dict(size=16),
                    ),
                    yaxis=dict(
                        title="Perda (%)",
                        title_font=dict(size=16),
                        tickfont=dict(size=16),
                    ),
                ),
            )

            st.plotly_chart(fig, use_container_width=True)

        i += 1 if i == 0 else -1


def plot_loss_per_csi_scatter() -> None:
    def plot_scatter(
        loss_table: pd.DataFrame,
        title: str,
        type: str,
        color: str,
        step: float = 0.1,
    ) -> None:
        if type not in ["interval", "quantile"]:
            raise ValueError(f"Tipo inválido '{type}'. Use 'interval' ou 'quantile'.")

        for angle, col in zip([-60, 60, 0], st.columns(2) + [st.columns([1, 2, 1])[1]]):
            angle_table = loss_table[loss_table["Angulação (°)"] == angle]

            match (type):
                case "interval":
                    grouped_table = calculate_interval(angle_table, step)
                case "quantile":
                    grouped_table = calculcate_quantile(angle_table, step)

            title_with_angle = f"{title} para um ângulo de {angle}°"

            fig_interval = create_grouped_csi_scatter(
                grouped_table, title_with_angle, color
            )

            with col:
                st.plotly_chart(fig_interval, use_container_width=True)

    loss_table = st.session_state.loss_table

    step = 0.1

    st.title("Comportamento das perdas para intervalos de CSI")
    plot_scatter(
        loss_table, f"Perda por intervalo de {step}", "interval", "#2bace9", step
    )

    st.title("Comportamento das perdas para quantis de CSI")
    plot_scatter(
        loss_table,
        f"Perda por quantil de {step * 100:.0f}%",
        "quantile",
        "#c8c3f8",
        step,
    )


def loss_table_page() -> None:
    start_page()
    header()
    plot_loss_per_angle_bar()
    plot_loss_per_csi_scatter()
