from io import BytesIO
import pandas as pd
import streamlit as st
from pyxlsb import open_workbook as open_xlsb

from scripts.detonaciones import generar_detonaciones

def show_detonaciones_view():
    st.title('Generación de detonaciones')
    
    uploaded_files = st.file_uploader("Carga tus documentos aquí", type= ['csv', 'xlsx'], accept_multiple_files=True)
    
    if uploaded_files is not None:
        st.divider()
        
        df_tel: pd.DataFrame = None
        cols_tel: list = []
        df_demo: pd.DataFrame = None
        col_cp: str = None
        df_RFC: pd.DataFrame = None
        col_RFC: str = None
        df_nombres: pd.DataFrame = None
        col_nombres: str = None
        df_email: pd.DataFrame = None
        col_email: str = None
        col_id_credit: str = None
        
        file_names = [file.name for file in uploaded_files]

        if len(uploaded_files) > 0:            
            #? DATOS TELEFÓNICOS
            st.subheader("Datos telefónicos")
            file_telefonico_name = st.selectbox("¿Qué archivo contiene los números telefónicos?", file_names)
            
            if file_telefonico_name:
                # Obtener el archivo seleccionado por su nombre
                file_tel = [file for file in uploaded_files if file.name == file_telefonico_name][0]

                if file_telefonico_name.endswith(".csv"):
                    df_tel = pd.read_csv(file_tel)
                    cols_tel = st.multiselect("¿Qué columnas contienen los números telefónicos?", df_tel.columns)
                elif file_telefonico_name.endswith(".xlsx"):
                    base_tel = pd.ExcelFile(file_tel)
                    base_tel_sheets = base_tel.sheet_names
                    # Mostrar un menú desplegable con las hojas disponibles
                    base_tel_selected_sheet = st.selectbox("Seleccione la hoja donde se encuentran los números telefónicos:", base_tel_sheets)
                    df_tel = pd.read_excel(file_tel, sheet_name=base_tel_selected_sheet)
                    cols_tel = st.multiselect("¿Qué columnas contienen los números telefónicos?", df_tel.columns)
                
                validar_whatsapp = st.checkbox('¿Desea validar si los números telefónicos móviles tienen WhatsApp?')
                            
            #? DATOS DEMOGRÁFICOS
            st.divider()
            st.subheader("Validación demográfica")
            st.write("Esta validación permite verificar que los números telefónicos coincidan con la información demográfica actual del cliente basándonos en su código postal.")
            validar_demograficamente = st.checkbox('¿Desea hacer una validación entre los números de teléfono y su información demográfica?')
            
            if validar_demograficamente:
                file_demografico_name = st.selectbox("¿Qué archivo contiene la información demográfica?", file_names)
                
                if file_demografico_name:
                    # Obtener el archivo seleccionado por su nombre
                    file_demo = [file for file in uploaded_files if file.name == file_demografico_name][0]

                    if file_demografico_name.endswith(".csv"):
                        df_demo = pd.read_csv(file_demo)
                        col_cp = st.selectbox("¿Qué columna contiene el código postal?", df_demo.columns)
                    elif file_demografico_name.endswith(".xlsx"):
                        base_demo = pd.ExcelFile(file_demo)
                        base_demo_sheets = base_demo.sheet_names
                        # Mostrar un menú desplegable con las hojas disponibles
                        base_demo_selected_sheet = st.selectbox("Seleccione la hoja donde se encuentran los datos demográficos:", base_demo_sheets)
                        df_demo = pd.read_excel(file_demo, sheet_name=base_demo_selected_sheet)
                        col_cp = st.selectbox("¿Qué columna contiene el código postal?", df_demo.columns)
            else:
                df_demo = None
                col_cp = None
            
            #? DATOS DE RFC
            st.divider()
            st.subheader("Validación de RFC")
            st.write("Esta validación generará dos columnas adicionales: 'Fecha de nacimiento' y 'Edad'.")
            validar_RFC = st.checkbox('¿Desea hacer una validación de RFC?')
            
            if validar_RFC:
                file_RFC_name = st.selectbox("¿Qué archivo contiene los RFC?", file_names)
                
                if file_RFC_name:
                    # Obtener el archivo seleccionado por su nombre
                    file_RFC = [file for file in uploaded_files if file.name == file_RFC_name][0]

                    if file_RFC_name.endswith(".csv"):
                        df_RFC = pd.read_csv(file_RFC)
                        col_RFC = st.selectbox("¿Qué columna contiene los RFC?", df_RFC.columns)
                    elif file_RFC_name.endswith(".xlsx"):
                        base_RFC = pd.ExcelFile(file_RFC)
                        base_RFC_sheets = base_RFC.sheet_names
                        # Mostrar un menú desplegable con las hojas disponibles
                        base_RFC_selected_sheet = st.selectbox("Seleccione la hoja donde se encuentran los RFC:", base_RFC_sheets)
                        df_RFC = pd.read_excel(file_RFC, sheet_name=base_RFC_selected_sheet)
                        col_RFC = st.selectbox("¿Qué columna contiene los RFC?", df_RFC.columns)
            else:
                df_RFC = None
                col_RFC = None
                        
            #? SEPARACIÓN DE NOMBRES
            st.divider()
            st.subheader("Separación de nombres")
            st.write("Esta proceso separa el nombre en cuatro columnas.")
            separar_nombres = st.checkbox('¿Desea separar los nombres?')
            
            if separar_nombres:
                file_nombres_name = st.selectbox("¿Qué archivo contiene los nombres?", file_names)
                
                if file_nombres_name:
                    # Obtener el archivo seleccionado por su nombre
                    file_nombres = [file for file in uploaded_files if file.name == file_nombres_name][0]

                    if file_nombres_name.endswith(".csv"):
                        df_nombres = pd.read_csv(file_nombres)
                        col_nombres = st.selectbox("¿Qué columna contiene los nombres?", df_nombres.columns)
                    elif file_nombres_name.endswith(".xlsx"):
                        base_nombres = pd.ExcelFile(file_nombres)
                        base_nombres_sheets = base_nombres.sheet_names
                        # Mostrar un menú desplegable con las hojas disponibles
                        base_nombres_selected_sheet = st.selectbox("Seleccione la hoja donde se encuentran los nombres:", base_nombres_sheets)
                        df_nombres = pd.read_excel(file_nombres, sheet_name=base_nombres_selected_sheet)
                        col_nombres = st.selectbox("¿Qué columna contiene los nombres?", df_nombres.columns)
            else:
                df_nombres = None
                col_nombres = None
                
            
            #? VALIDACIÓN DE CORREO ELECTRÓNICO
            st.divider()
            st.subheader("Validación de correo electrónico")
            st.write("Esta validación añade el email al archivo de detonaciones.")
            validar_correo = st.checkbox('¿Desea validar el correo electrónico?')
            
            if validar_correo:
                file_email_name = st.selectbox("¿Qué archivo contiene los correos electrónicos?", file_names)
                
                if file_email_name:
                    # Obtener el archivo seleccionado por su nombre
                    file_email = [file for file in uploaded_files if file.name == file_email_name][0]

                    if file_email_name.endswith(".csv"):
                        df_email = pd.read_csv(file_email)
                        col_email = st.selectbox("¿Qué columna contiene los correos electrónicos?", df_email.columns)
                    elif file_email_name.endswith(".xlsx"):
                        base_email = pd.ExcelFile(file_email)
                        base_email_sheets = base_email.sheet_names
                        # Mostrar un menú desplegable con las hojas disponibles
                        base_email_selected_sheet = st.selectbox("Seleccione la hoja donde se encuentran los correos electrónicos:", base_email_sheets)
                        df_email = pd.read_excel(file_email, sheet_name=base_email_selected_sheet)
                        col_email = st.selectbox("¿Qué columna contiene los correos electrónicos?", df_email.columns)
            else:
                df_email = None
                col_email = None
            
            #? COLUMNA IDENTIFICADORA
            st.divider()
            st.subheader("Columna identificadora")
            st.write("Seleccione la columna identifican a cada cliente por su crédito en cada uno de los documentos seleccionados.")
            
            selected_columns = [
               df_tel.columns if df_tel is not None else None,
               df_demo.columns if df_demo is not None else None,
               df_RFC.columns if df_RFC is not None else None,
               df_nombres.columns if df_nombres is not None else None,
            ]

            selected_columns = list(set([item for sublist in selected_columns if sublist is not None for item in sublist]))
            
            col_id_credit = st.selectbox("Seleccione la columna identificadora:", selected_columns)
            
            st.divider()
            
            if st.button('Generar detonaciones'):
                df_detonaciones = generar_detonaciones(
                    col_id_credit=col_id_credit,
                    df_numbers=df_tel,
                    df_demo=df_demo, 
                    df_nombres=df_nombres,
                    df_RFC=df_RFC,
                    col_cp=col_cp,
                    campaña="Bancoppel",
                    col_nombres=col_nombres,
                    col_RFC=col_RFC,
                    cols_tel=cols_tel,   
                    need_demographic_validation=validar_demograficamente,
                    need_rfc_validation=validar_RFC,
                    need_whatsapp_validation=validar_whatsapp,
                    need_separete_names=separar_nombres,
                    need_email_validation=validar_correo,
                    df_email=df_email,
                    col_email=col_email  
                )
                
                if df_detonaciones is not None:
                    st.divider()
                    st.write("¡Detonaciones generadas!")
                    
                    def to_excel(df):
                        output = BytesIO()
                        writer = pd.ExcelWriter(output, engine='xlsxwriter')
                        df.to_excel(writer, index=False, sheet_name='Detonaciones')
                        workbook = writer.book
                        worksheet = writer.sheets['Detonaciones']
                        format1 = workbook.add_format({'num_format': '0.00'}) 
                        worksheet.set_column('A:A', None, format1)  
                        writer.close()
                        processed_data = output.getvalue()
                        return processed_data
                    
                    df_xlsx = to_excel(df_detonaciones)

                    st.download_button(
                        label="Descargar como Excel",
                        data=df_xlsx,
                        file_name='detonaciones.xlsx',
                    )