import streamlit as st
from modules.clean.clean_view import show_clean_view
from modules.detonaciones.detonaciones_view import show_detonaciones_view
from modules.home.home_view import show_home_view


def main():
    st.sidebar.title('Menú')
    menu_option = st.sidebar.selectbox(
        'Selecciona una opción', 
        [
            'Inicio', 
            'Limpieza de datos',
            'Detonaciones'
        ]
    )

    if menu_option == 'Inicio':
        show_home_view()

    elif menu_option == 'Limpieza de datos':
        show_clean_view()
        
    elif menu_option == 'Detonaciones':
        show_detonaciones_view()
        

if __name__ == "__main__":
    main()