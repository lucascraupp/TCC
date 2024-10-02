import streamlit as st
from screens.pyranometer_page import pyranometer_page
from streamlit_option_menu import option_menu

# Configuração de metadados da página
st.set_page_config(
    page_title="TCC Dashboard",
    page_icon="services/resources/assets/sol.png",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Cria navegação entre as páginas
page = option_menu(
    menu_title=None,
    options=[
        "Página inicial",
        "Piranômetros",
    ],
    icons=[
        "house",
        "sunrise",
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
        case "Piranômetros":
            pyranometer_page()
