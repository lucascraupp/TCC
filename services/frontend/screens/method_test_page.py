import json

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

PLANTS_PARAM = json.load(open("resources/solar_plants.json"))
MONTHS = {
    "Período inteiro": 0,
    "Janeiro": 1,
    "Fevereiro": 2,
    "Março": 3,
    "Abril": 4,
    "Maio": 5,
    "Junho": 6,
}


def get_data() -> None:
    data = st.session_state.data
    month = MONTHS[st.session_state.month]

    if month != 0:
        loss_table = data[data["Data"].dt.month == month]
    else:
        loss_table = data

    st.session_state.loss_table = loss_table


def generate_block(value: float, title: str, color: str) -> str:
    return f"""
            <div style="width: 100%;
                        background-color: {color};
                        text-align: center;
                        color: white;
                        padding: 8px;
                        border-radius: 8px;
                        margin: 0 0 32px 0;
                        display: flex;
                        flex-direction: column;
                        justify-content: center;
                        ">
                <p style="margin: auto;">{title}</p>
                <p style="margin: auto; font-weight: 600; font-size: 1.75em">{value}% </p>
            </div>
            """


def start_page() -> None:
    def generate_keys(defaults: dict) -> None:
        # Inicia estado da sessão
        for key in defaults:
            if key not in st.session_state:
                st.session_state[key] = defaults[key]

    defaults = {
        "page": "method_test_page",
        "first_start": True,
        "month": "Período inteiro",
    }

    generate_keys(defaults)

    if st.session_state.page != "method_test_page":
        st.session_state.clear()
        generate_keys(defaults)

    if st.session_state.first_start:
        data = pd.read_parquet(
            PLANTS_PARAM["Hélio"]["datawarehouse"]["loss_due_to_unavailability"]
        )
        data["Data"] = pd.to_datetime(data["Data"])
        data = (
            data.groupby("Data")
            .agg(
                {
                    "CSI": "first",
                    "Porcentagem de indisponibilidade (%)": "sum",
                    "Perda por indisponibilidade (%)": "sum",
                }
            )
            .reset_index()
        )

        st.session_state.data = data

        st.session_state.first_start = False

        get_data()


def header() -> None:
    st.selectbox(
        "Selecione o mês",
        options=list(MONTHS.keys()),
        key="month",
        on_change=get_data,
    )


def plot_day_loss_bar() -> None:
    loss_table = st.session_state.loss_table
    month = st.session_state.month

    st.title(f"Perda diária devido à indisponibilidade - {month}")

    col = st.columns(2)

    with col[0]:
        loss_mean = round(loss_table["Perda por indisponibilidade (%)"].mean(), 2)

        st.markdown(
            generate_block(
                loss_mean,
                f"Perda média estimada em relação à geração de {st.session_state.month}",
                "#FF6961",
            ),
            unsafe_allow_html=True,
        )
    with col[1]:
        indisponibility_mean = round(
            loss_table["Porcentagem de indisponibilidade (%)"].mean(), 2
        )

        st.markdown(
            generate_block(
                indisponibility_mean,
                f"Porcentagem média de indisponibilidade em relação à geração de {st.session_state.month}",
                "#ce877d",
            ),
            unsafe_allow_html=True,
        )

    fig = go.Figure(
        data=[
            go.Bar(
                x=loss_table["Data"],
                y=loss_table["Perda por indisponibilidade (%)"],
                name="Perda estimada (%)",
                marker=dict(color="#FF6961"),
                hovertemplate=(
                    "<b>Data</b>: %{x}<br>"
                    "<b>CSI</b>: %{customdata[0]}<br>"
                    "<b>Porcentagem de indisponibilidade</b>: %{customdata[1]:.2f}%<br>"
                    "<b>Perda por indisponibilidade</b>: %{y:.2f}%<extra></extra>"
                ),
                customdata=loss_table[["CSI", "Porcentagem de indisponibilidade (%)"]],
            )
        ],
        layout=go.Layout(
            title="Perda diária devido à indisponibilidade de trackers",
            titlefont=dict(size=16),
            xaxis=dict(
                title="Data",
                title_font=dict(size=16),
                tickfont=dict(size=16),
            ),
            yaxis=dict(
                title="Perda estimada (%)",
                title_font=dict(size=16),
                tickfont=dict(size=16),
            ),
            legend=dict(font=dict(size=16)),
        ),
    )

    st.plotly_chart(fig, use_container_width=True)


def method_test_page() -> None:
    start_page()
    header()
    plot_day_loss_bar()
