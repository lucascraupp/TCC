import streamlit as st
from screens.loss_table_page import loss_table_page
from screens.method_test_page import method_test_page
from screens.pyranometer_page import pyranometer_page
from streamlit_option_menu import option_menu

# Configuração de metadados da página
st.set_page_config(
    page_title="TCC Dashboard",
    page_icon="resources/assets/sol.png",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Cria navegação entre as páginas
page = option_menu(
    menu_title=None,
    options=[
        "Página inicial",
        "Piranômetros",
        "Análise das perdas",
        "Comparação entre o método e a biblioteca",
    ],
    icons=[
        "house",
        "sunrise",
        "database-fill",
        "bi-file-earmark-bar-graph",
    ],
    default_index=0,
    orientation="horizontal",
    key="om_solar",
    styles={
        "nav-link-selected": {"background-color": "#00aaff"},
    },
)

# Renderiza a página selecionada
if __name__ == "__main__":
    match page:
        case "Página inicial":
            st.session_state.clear()
        case "Piranômetros":
            pyranometer_page()
        case "Análise das perdas":
            loss_table_page()
        case "Comparação entre o método e a biblioteca":
            method_test_page()
