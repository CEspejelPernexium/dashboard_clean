import streamlit as st
import pandas as pd

from scripts.vid import clean_databases

def show_clean_view():

    st.title('Limpieza de Datos')
    st.write('Estamos trabajando en ello...')
    
    # uploaded_files = st.file_uploader("Cargar archivo", type= ['csv', 'xlsx'], accept_multiple_files=True)
    
    # # Si se cargó un archivo, mostrar información sobre él
    # if uploaded_files is not None:
    #     st.divider()
        
    #     df_tel: pd.DataFrame = None
    #     cols_tel: list = []
    #     df_demo: pd.DataFrame = None
    #     col_cp: str = None
    #     df_RFC: pd.DataFrame = None
    #     col_RFC: str = None
    #     df_nombres: pd.DataFrame = None
    #     col_nombres: str = None
    #     df_productos: pd.DataFrame = None
    #     cols_productos: list = []
    #     cols_tel: str = None
    #     cols_productos: str = None
    #     col_id_client: str = None
    #     col_product_id: str = None
    #     col_cp: str = None
    #     col_RFC: str = None
    #     col_nombres: str = None
        
    #     file_names = [file.name for file in uploaded_files]

    #     if len(uploaded_files) > 0:            
    #         #? DATOS TELEFÓNICOS
    #         st.subheader("Datos telefónicos")
    #         file_telefonico_name = st.selectbox("¿Qué archivo contiene los números telefónicos?", file_names)
            
    #         if file_telefonico_name:
    #             # Obtener el archivo seleccionado por su nombre
    #             file_tel = [file for file in uploaded_files if file.name == file_telefonico_name][0]

    #             if file_telefonico_name.endswith(".csv"):
    #                 df_tel = pd.read_csv(file_tel)
    #                 cols_tel = st.multiselect("¿Qué columnas contienen los números telefónicos?", df_tel.columns)
    #             elif file_telefonico_name.endswith(".xlsx"):
    #                 base_tel = pd.ExcelFile(file_tel)
    #                 base_tel_sheets = base_tel.sheet_names
    #                 # Mostrar un menú desplegable con las hojas disponibles
    #                 base_tel_selected_sheet = st.selectbox("Seleccione la hoja donde se encuentran los números telefónicos:", base_tel_sheets)
    #                 df_tel = pd.read_excel(file_tel, sheet_name=base_tel_selected_sheet)
    #                 cols_tel = st.multiselect("¿Qué columnas contienen los números telefónicos?", df_tel.columns)
                
    #             validar_whatsapp = st.checkbox('¿Desea validar si los números telefónicos móviles tienen WhatsApp?')
                            
    #         #? DATOS DEMOGRÁFICOS
    #         st.divider()
    #         st.subheader("Validación demográfica")
    #         st.write("Esta validación permite verificar que los números telefónicos coincidan con la información demográfica actual del cliente basándonos en su código postal.")
    #         validar_demograficamente = st.checkbox('¿Desea hacer una validación entre los números de teléfono y su información demográfica?')
            
    #         if validar_demograficamente:
    #             file_demografico_name = st.selectbox("¿Qué archivo contiene la información demográfica?", file_names)
                
    #             if file_demografico_name:
    #                 # Obtener el archivo seleccionado por su nombre
    #                 file_demo = [file for file in uploaded_files if file.name == file_demografico_name][0]

    #                 if file_demografico_name.endswith(".csv"):
    #                     df_demo = pd.read_csv(file_demo)
    #                     col_cp = st.selectbox("¿Qué columna contiene el código postal?", df_demo.columns)
    #                 elif file_demografico_name.endswith(".xlsx"):
    #                     base_demo = pd.ExcelFile(file_demo)
    #                     base_demo_sheets = base_demo.sheet_names
    #                     # Mostrar un menú desplegable con las hojas disponibles
    #                     base_demo_selected_sheet = st.selectbox("Seleccione la hoja donde se encuentran los datos demográficos:", base_demo_sheets)
    #                     df_demo = pd.read_excel(file_demo, sheet_name=base_demo_selected_sheet)
    #                     col_cp = st.selectbox("¿Qué columna contiene el código postal?", df_demo.columns)
    #         else:
    #             df_demo = None
    #             col_cp = None
            
    #         #? DATOS DE RFC
    #         st.divider()
    #         st.subheader("Validación de RFC")
    #         st.write("Esta validación generará dos columnas adicionales: 'Fecha de nacimiento' y 'Edad'.")
    #         validar_RFC = st.checkbox('¿Desea hacer una validación de RFC?')
            
    #         if validar_RFC:
    #             file_RFC_name = st.selectbox("¿Qué archivo contiene los RFC?", file_names)
                
    #             if file_RFC_name:
    #                 # Obtener el archivo seleccionado por su nombre
    #                 file_RFC = [file for file in uploaded_files if file.name == file_RFC_name][0]

    #                 if file_RFC_name.endswith(".csv"):
    #                     df_RFC = pd.read_csv(file_RFC)
    #                     col_RFC = st.selectbox("¿Qué columna contiene los RFC?", df_RFC.columns)
    #                 elif file_RFC_name.endswith(".xlsx"):
    #                     base_RFC = pd.ExcelFile(file_RFC)
    #                     base_RFC_sheets = base_RFC.sheet_names
    #                     # Mostrar un menú desplegable con las hojas disponibles
    #                     base_RFC_selected_sheet = st.selectbox("Seleccione la hoja donde se encuentran los RFC:", base_RFC_sheets)
    #                     df_RFC = pd.read_excel(file_RFC, sheet_name=base_RFC_selected_sheet)
    #                     col_RFC = st.selectbox("¿Qué columna contiene los RFC?", df_RFC.columns)
    #         else:
    #             df_RFC = None
    #             col_RFC = None
                        
    #         #? SEPARACIÓN DE NOMBRES
    #         st.divider()
    #         st.subheader("Separación de nombres")
    #         st.write("Esta proceso separa el nombre en cuatro columnas.")
    #         separar_nombres = st.checkbox('¿Desea separar los nombres?')
            
    #         if separar_nombres:
    #             file_nombres_name = st.selectbox("¿Qué archivo contiene los nombres?", file_names)
                
    #             if file_nombres_name:
    #                 # Obtener el archivo seleccionado por su nombre
    #                 file_nombres = [file for file in uploaded_files if file.name == file_nombres_name][0]

    #                 if file_nombres_name.endswith(".csv"):
    #                     df_nombres = pd.read_csv(file_nombres)
    #                     col_nombres = st.selectbox("¿Qué columna contiene los nombres?", df_nombres.columns)
    #                 elif file_nombres_name.endswith(".xlsx"):
    #                     base_nombres = pd.ExcelFile(file_nombres)
    #                     base_nombres_sheets = base_nombres.sheet_names
    #                     # Mostrar un menú desplegable con las hojas disponibles
    #                     base_nombres_selected_sheet = st.selectbox("Seleccione la hoja donde se encuentran los nombres:", base_nombres_sheets)
    #                     df_nombres = pd.read_excel(file_nombres, sheet_name=base_nombres_selected_sheet)
    #                     col_nombres = st.selectbox("¿Qué columna contiene los nombres?", df_nombres.columns)
    #         else:
    #             df_nombres = None
    #             col_nombres = None
                        
    #         #? AGRUPACIÓN DE PRODUCTOS
    #         st.divider()
    #         st.subheader("Agrupación de productos")
    #         st.write("Esta proceso agrupa los productos de cada cliente en un solo registro.")
    #         agrupar_productos = st.checkbox('¿Desea agrupar los productos?')
            
    #         if agrupar_productos:
    #             file_productos_name = st.selectbox("¿Qué archivo contiene los productos?", file_names)
                
    #             if file_productos_name:
    #                 # Obtener el archivo seleccionado por su nombre
    #                 file_productos = [file for file in uploaded_files if file.name == file_productos_name][0]

    #                 if file_productos_name.endswith(".csv"):
    #                     df_productos = pd.read_csv(file_productos)
    #                     cols_productos = st.multiselect("¿Qué columnas contienen información de los productos del cliente?", df_productos.columns)
    #                 elif file_productos_name.endswith(".xlsx"):
    #                     base_productos = pd.ExcelFile(file_productos)
    #                     base_productos_sheets = base_productos.sheet_names
    #                     # Mostrar un menú desplegable con las hojas disponibles
    #                     base_productos_selected_sheet = st.selectbox("Seleccione la hoja donde se encuentran los productos:", base_productos_sheets)
    #                     df_productos = pd.read_excel(file_productos, sheet_name=base_productos_selected_sheet)
    #                     cols_productos = st.multiselect("¿Qué columnas contienen información de los productos del cliente?", df_productos.columns)
                        
    #                     if len(cols_productos) > 0:
    #                         col_product_id = st.selectbox("¿Qué columna contiene el identificador del producto?", cols_productos)
    #         else:
    #             df_productos = None
    #             cols_productos = None
            
    #         #? COLUMNA IDENTIFICADORA
    #         st.divider()
    #         st.subheader("Columna identificadora")
    #         st.write("Seleccione la columna o las columnas que identifican a cada cliente en cada uno de los documentos seleccionados.")
            
    #         selected_columns = [
    #            df_tel.columns if df_tel is not None else None,
    #            df_demo.columns if df_demo is not None else None,
    #            df_RFC.columns if df_RFC is not None else None,
    #            df_nombres.columns if df_nombres is not None else None,
    #            df_productos.columns if df_productos is not None else None
    #         ]
            
    #         selected_columns = list(set([item for sublist in selected_columns if sublist is not None for item in sublist]))
            
    #         col_id_client = st.selectbox("Seleccione la columna identificadora:", selected_columns)
            
    #         if st.button('Limpiar datos'):
    #             databases_unique = []

    #             # Itera sobre la lista de DataFrames y verifica si cada DataFrame es igual a los otros
    #             for df1 in [df_tel, df_demo, df_RFC, df_nombres, df_productos]:
    #                 # Si el DataFrame no es None y no es igual a ninguno de los DataFrames únicos, agrégalo a la lista de únicos
    #                 if df1 is not None and not any(df1.equals(df2) for df2 in databases_unique):
    #                     databases_unique.append(df1)
                
    #             clean_databases(
    #                 campaña= "Bancoppel",
    #                 databases = databases_unique,
    #                 need_demographic_validation = validar_demograficamente,
    #                 need_whatsapp_validation = validar_whatsapp,
    #                 need_separate_names = separar_nombres,
    #                 need_group_products_by_client = agrupar_productos,
    #                 need_validate_RFC = validar_RFC,
    #                 cols_tels = cols_tel,
    #                 cols_productos = cols_productos,
    #                 col_id_client = col_id_client,
    #                 col_id_product = col_product_id,
    #                 col_cp = col_cp,
    #                 col_RFC = col_RFC,
    #                 col_name = col_nombres,
    #             )
            
            
            
            
