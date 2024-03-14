import pandas as pd
import numpy as np
import warnings
import sys
import time
warnings.filterwarnings("ignore")
import pandas as pd
import mysql.connector
import requests
from datetime import datetime
import streamlit as st

doc_IFT = "documents/df_ift.csv"
doc_SEPOMEX = "documents/df_sepomex.txt"

equivalencias_txt = "AGS|Aguascalientes|BC|Baja California|BCS|Baja California Sur|CAMP|Campeche|CDMX|Ciudad de México|CHIH|Chihuahua|CHIS|Chiapas|COAH|Coahuila de Zaragoza|COL|Colima|DGO|Durango|ERROR|Error|GRO|Guerrero|GTO|Guanajuato|HGO|Hidalgo|JAL|Jalisco|MEX|México|MICH|Michoacán de Ocampo|MOR|Morelos|NAY|Nayarit|NL|Nuevo León|OAX|Oaxaca|PUE|Puebla|QRO|Querétaro|QROO|Quintana Roo|SIN|Sinaloa|SLP|San Luis Potosí|SON|Sonora|TAB|Tabasco|TAMPS|Tamaulipas|TLAX|Tlaxcala|VER|Veracruz de Ignacio de la Llave|YUC|Yucatán|ZAC|Zacatecas"
equivalencias_list = equivalencias_txt.split('|')
equivalencias_dict = {equivalencias_list[i + 1]: equivalencias_list[i] for i in range(0, len(equivalencias_list), 2)}

df_IFT = pd.read_csv(doc_IFT)
df_IFT['NIR_SERIE'] = df_IFT['NIR_SERIE'].astype(str)

columns = ['d_codigo', 'd_asenta', 'd_tipo_asenta', 'D_mnpio', 'd_estado', 'd_ciudad', 'd_CP', 'c_estado', 'c_oficina', 'c_CP', 'c_tipo_asenta', 'c_mnpio', 'id_asenta_cpcons', 'd_zona', 'c_cve_ciudad']
df_sepomex = pd.read_csv(doc_SEPOMEX, sep="|", header=0, names=columns, encoding='ISO-8859-1')

#! CONEXIÓN A LA BASE DE DATOS
def getDBConnection():
     # Configuración de la conexión a la base de datos
    conn = mysql.connector.connect(
        host='pernexium-db.cfioetbrvik6.us-east-2.rds.amazonaws.com',
        user='analitica_alain',
        password="gBi1{H120U3DT@c",
        database='analitica',
    )
    return conn

#! VALIDAR NÚMERO TELEFÓNICO
def validate_number(number):
    """Valida y limpia un número de teléfono

    Args:
        number (Any): Número de teléfono a validar y limpiar

    Returns:
        str: Número de teléfono limpio, 
    """
    # Quitar los caracteres que no son dígitos.
    numero_limpio = ''.join(c for c in str(number) if c.isdigit())
    # Si la longitud es menor a 10, regresar "INCOMPLETO".
    if len(numero_limpio) < 10:
        return 'INCOMPLETO'
    # Si el número tiene más de 10 dígitos, tomar solo los últimos 10.
    return numero_limpio[-10:]

#! VALIDAR LA EXISTENCIA DEL NÚMERO EN WHATSAPP
def verify_whatsapp_number(tel: str):
    """Valida la existencia de un número de teléfono en WhatsApp
    Args:
        tel (str): Número de teléfono a validar
    Returns:
        bool: True si el número existe en WhatsApp, False en caso contrario
    """
    url = "http://ec2-3-141-98-139.us-east-2.compute.amazonaws.com/validate"

    response = requests.post(url, json={"phoneNumber": tel})
    data = response.json()
    exists = data["exists"]
    
    return exists

#! VALIDACIÓN DEMOGRÁFICA CON CÓDIGO POSTAL
def validate_demographic_info(cp: str, df_sepomex: pd.DataFrame, estado: str):
    """Valida la información demográfica de un código postal

    Args:
        cp (str): Código postal a validar
        df_sepomex (pd.DataFrame): DataFrame con la información de los códigos postales
        estado (str): Estado al que pertenece el código postal

    Returns:
        _type_: Regresamos el estado_sepomex, estado_abreviatura y equivalente_correcto
    """
    if pd.notna(cp) and cp != 0:
        query_result = df_sepomex.query(f"d_codigo == {int(cp)}")
                                        
        if not query_result.empty:
            estado_sepomex = query_result['d_estado'].iloc[0]
        else:
            estado_sepomex = 'Desconocido'
    elif pd.isna(cp):
        estado_sepomex = 'Código Postal NaN'
    else:
        estado_sepomex = 'Código Postal 0'
                          
    if estado_sepomex == 'Desconocido' or estado_sepomex == 'Código Postal NaN' or estado_sepomex == 'Código Postal 0':
        equivalente_correcto = False
        estado_abreviatura = 'Desconocido'
    else:
        if estado_sepomex not in equivalencias_dict:
            equivalente_correcto = False
            estado_abreviatura = f'Desconocido - {estado_sepomex}'
        else:
            estado_abreviatura = equivalencias_dict[estado_sepomex]
            equivalente_correcto = estado_abreviatura == estado
                                    
    return estado_sepomex, estado_abreviatura, equivalente_correcto

#! VALIDAR TODOS LOS NÚMEROS TELEFÓNICOS
def clean_phone_numbers(
    df_numbers: pd.DataFrame, 
    col_id_credit: str,
    need_demographic_validation: bool, 
    need_whatsapp_validation: bool,
    cols_tels: list,  
    df_demografico: pd.DataFrame = None,
    cp_column: str = None,
    ):
    
    df_numbers['TELEFONOS'] = None
             
    c = 0
    
    for col in cols_tels:
        if df_numbers[col].dtype == np.float64:
            df_numbers[col] = df_numbers[col].apply(lambda x: x if pd.isnull(x) else str(int(x)))
            
    # Mostrar el progreso
    progress_text = "Validando números."
        
    if need_demographic_validation:
        progress_text = "Validando números con información demográfica"
        
    my_bar = st.progress(0, text=progress_text)

    # Iterrar los números telefónicos
    for index, row in df_numbers.iterrows():
        c += 1
        tels = []
        cp = None
        processed_numbers = {}
        
        my_bar.progress(value=c/len(df_numbers), text=progress_text)
                
        
        # Obtener el código postal del DataFrame de la información demográfica
        if need_demographic_validation and df_demografico is not None and cp_column is not None and cp_column in df_demografico:
            try:
                id_credit = df_numbers.at[index, col_id_credit]
                # Buscar el Código Postal del cliente en el DataFrame de la información demográfica, solo obtener el primer resultado
                cp = df_demografico[df_demografico[col_id_credit] == id_credit][cp_column].values[0]
                if cp_column not in df_numbers.columns:
                    df_numbers.at[index, cp_column] = cp
            except:
                cp = None
        
        # Iterar columnas telefónicas
        for col in cols_tels:
            if col not in df_numbers.columns:
                continue
            
            original_phone_number = row[col]
            
            # Validar si el número ya fue procesado
            if original_phone_number in processed_numbers:
                # Tomar la respuesta del número ya procesado
                phone_info = processed_numbers[original_phone_number]
                phone_info['SOURCE'] = col  # Actualizar la fuente del número procesado
                tels.append(phone_info)


            # Validar número telefónico
            clean_phone_number = validate_number(original_phone_number)
            
            # Si el número es incompleto
            if clean_phone_number == 'INCOMPLETO':
                phone_info = {
                    'TELEFONO': original_phone_number,
                    'SOURCE': col,
                    'TIPO_RED': 'ERROR',
                    'POBLACION': 'ERROR',
                    'MUNICIPIO': 'ERROR',
                    'ESTADO': 'ERROR',
                    'ESTADO SEPOMEX': 'ERROR',
                    'ESTADO ABREVIATURA': 'ERROR',
                    'EQUIVALENTE CORRECTO': 'ERROR',
                    'RAZON_SOCIAL': 'ERROR',
                    'WHATSAPP': 'ERROR',
                    'RAZÓN DE ERROR': 'NÚMERO INCOMPLETO'
                }
                
                tels.append(phone_info)
                processed_numbers[original_phone_number] = phone_info
            # Si el número es válido
            else: 
                # Buscar el número en el archivo de la IFT            
                results_IFT = df_IFT[df_IFT['NIR_SERIE'].str.startswith(clean_phone_number[:6])]
            
                # Si hay más de un resultado
                if len(results_IFT) > 1:
                    
                    # Reducir el número a 4 dígitos
                    reduced_number = int(clean_phone_number[-4:])
                    founded = False
                    
                    # Iterar resultados
                    for _, row_IFT in results_IFT.iterrows():
                        # Si el número está entre el rango de la numeración inicial y final
                        if (reduced_number > int(row_IFT[' NUMERACION_INICIAL'])) and (reduced_number < int(row_IFT[' NUMERACION_FINAL'])):
                            founded = True
                            
                            tipo_red = row_IFT[' TIPO_RED']
                            exists_in_whatsapp = False
                            
                            # Verificar si el número tiene WhatsApp si es de tipo móvil
                            if tipo_red == 'MOVIL' and need_whatsapp_validation:
                                exists_in_whatsapp = verify_whatsapp_number(clean_phone_number)
                            else:
                                exists_in_whatsapp = 'No requerido'
                                
                            estado_sepomex = 'No requerido'
                            estado_abreviatura = 'No requerido'
                            equivalente_correcto = 'No requerido'
                                
                            # Validar si necesita información demográfica y que se recibieron correctamente los datos
                            if need_demographic_validation:
                                if cp is not None and df_sepomex is not None:
                                    estado_sepomex, estado_abreviatura, equivalente_correcto = validate_demographic_info(cp, df_sepomex, row_IFT[' ESTADO'])
                                else:
                                    estado_sepomex = 'No se encontró información de Código Postal'
                                    estado_abreviatura = 'No se encontró información de Código Postal'
                                    equivalente_correcto = 'No se encontró información de Código Postal'
                                
                            phone_info = {
                                'TELEFONO': clean_phone_number,
                                'SOURCE': col,
                                'TIPO_RED': row_IFT[' TIPO_RED'],
                                'POBLACION': row_IFT[' POBLACION'],
                                'MUNICIPIO': row_IFT[' MUNICIPIO'],
                                'ESTADO': row_IFT[' ESTADO'],
                                'ESTADO SEPOMEX': estado_sepomex,
                                'ESTADO ABREVIATURA': estado_abreviatura,
                                'EQUIVALENTE CORRECTO': equivalente_correcto,
                                'RAZON_SOCIAL': row_IFT[' RAZON_SOCIAL'],
                                'WHATSAPP': exists_in_whatsapp
                            }
                            
                            tels.append(phone_info)
                            processed_numbers[original_phone_number] = phone_info
                            break
                        
                        # Si no se encuentra el número
                    if not founded:
                        phone_info = {
                            'TELEFONO': clean_phone_number,
                            'SOURCE': col,
                            'TIPO_RED': 'ERROR',
                            'POBLACION': 'ERROR',
                            'MUNICIPIO': 'ERROR',
                            'ESTADO': 'ERROR',
                            'ESTADO SEPOMEX': 'ERROR',
                            'ESTADO ABREVIATURA': 'ERROR',
                            'EQUIVALENTE CORRECTO': 'ERROR',
                            'RAZON_SOCIAL': 'ERROR',
                            'WHATSAPP': 'ERROR',
                            'RAZÓN DE ERROR': 'NÚMERO NO ENCONTRADO EN RANGO DE NUMERACIÓN'
                        }
                            
                        tels.append(phone_info)
                        processed_numbers[original_phone_number] = phone_info
                
                # Si hay un solo resultado         
                elif not results_IFT.empty:
                    tipo_red = results_IFT[' TIPO_RED'].values[0]
                    exists_in_whatsapp = False
                    
                    # Verificar si el número tiene WhatsApp si es de tipo móvil      
                    if tipo_red == 'MOVIL' and need_whatsapp_validation:
                        exists_in_whatsapp = verify_whatsapp_number(clean_phone_number)
                    else:
                        exists_in_whatsapp = 'No requerido'
                        
                    estado_sepomex = 'No requerido'
                    estado_abreviatura = 'No requerido'
                    equivalente_correcto = 'No requerido'
                                
                    # Validar si necesita información demográfica
                    if need_demographic_validation:
                        # Validar que se recibieron correctamente los datos
                        if cp is not None and df_sepomex is not None:
                            estado_sepomex, estado_abreviatura, equivalente_correcto = validate_demographic_info(cp, df_sepomex, results_IFT[' ESTADO'].values[0])
                        else:
                            estado_sepomex = 'No se encontró información de Código Postal'
                            estado_abreviatura = 'No se encontró información de Código Postal'
                            equivalente_correcto = 'No se encontró información de Código Postal'
                    
                    phone_info = {
                        'TELEFONO': clean_phone_number,
                        'SOURCE': col,
                        'TIPO_RED': results_IFT[' TIPO_RED'].values[0],
                        'POBLACION': results_IFT[' POBLACION'].values[0],
                        'MUNICIPIO': results_IFT[' MUNICIPIO'].values[0],
                        'ESTADO': results_IFT[' ESTADO'].values[0],
                        'ESTADO SEPOMEX': estado_sepomex,
                        'ESTADO ABREVIATURA': estado_abreviatura,
                        'EQUIVALENTE CORRECTO': equivalente_correcto,
                        'RAZON_SOCIAL': results_IFT[' RAZON_SOCIAL'].values[0],
                        'WHATSAPP': exists_in_whatsapp
                    }
                    
                    tels.append(phone_info)
                    processed_numbers[original_phone_number] = phone_info
                    continue

                # Si no se encuentra el número
                elif results_IFT.empty:
                    phone_info = {
                        'TELEFONO': clean_phone_number,
                        'SOURCE': col,
                        'TIPO_RED': 'ERROR',
                        'POBLACION': 'ERROR',
                        'MUNICIPIO': 'ERROR',
                        'ESTADO': 'ERROR',
                        'ESTADO SEPOMEX': 'ERROR',
                        'ESTADO ABREVIATURA': 'ERROR',
                        'EQUIVALENTE CORRECTO': 'ERROR',
                        'RAZON_SOCIAL': 'ERROR',
                        'WHATSAPP': 'ERROR',
                        'RAZÓN DE ERROR': 'NÚMERO NO ENCONTRADO EN EL ARCHIVO DE LA IFT'
                    }
                    
                    tels.append(phone_info)
                    processed_numbers[original_phone_number] = phone_info
                    continue
        
        # Asignar los números telefónicos al dataframe     
        df_numbers.at[index, 'TELEFONOS'] = tels
    
    df_numbers = df_numbers.drop(columns=cols_tels)

    return df_numbers

def get_contactable_numbers(df_numbers_clean: pd.DataFrame):
    total_tel_fijos = 0
    total_tel_moviles = 0
    total_tel_con_whatsapp = 0
    total_tel_sin_whatsapp = 0
    total_tel_no_contactable = 0
    
    my_bar = st.progress(0, text="Obteniendo números contactables")
    
    total_iterations = len(df_numbers_clean)
    
    for index, row in df_numbers_clean.iterrows():
        c = index + 1
        
        my_bar.progress(value=c/total_iterations, text="Obteniendo números contactables")
        
        tels = row['TELEFONOS']
        
        tel_info_1 = None
        tel_info_2 = None
        tel_info_3 = None
        cp_1 = None
        cp_2 = None
        cp_3 = None

        
        for tel in tels:
            if tel_info_1 is None and tel['TIPO_RED'] == 'MOVIL' and tel['WHATSAPP'] == True:
                tel_info_1 = tel['TELEFONO']
                cp_1 = tel['EQUIVALENTE CORRECTO']
            elif tel_info_2 is None and tel['TIPO_RED'] == 'MOVIL' and tel['WHATSAPP'] == True:
                tel_info_2 = tel['TELEFONO']
                cp_2 = tel['EQUIVALENTE CORRECTO']
            elif tel_info_3 is None and tel['TIPO_RED'] == 'MOVIL' and tel['WHATSAPP'] == True:
                tel_info_3 = tel['TELEFONO']
                cp_3 = tel['EQUIVALENTE CORRECTO']
            
            if tel['TIPO_RED'] == 'FIJO':
                total_tel_fijos += 1
                total_tel_no_contactable += 1
                continue
            elif tel['TIPO_RED'] == 'MOVIL':
                total_tel_moviles += 1
                if tel['WHATSAPP'] == True:
                    total_tel_con_whatsapp += 1
                    continue
                else:
                    total_tel_sin_whatsapp += 1
                    total_tel_no_contactable += 1
                    continue
            elif tel['TIPO_RED'] == 'ERROR':
                total_tel_no_contactable += 1
                continue
        
        if tel_info_1 is not None or tel_info_2 is not None or tel_info_3 is not None:
            df_numbers_clean.at[index, 'TEL1'] = tel_info_1 if tel_info_1 is not None else pd.NA
            df_numbers_clean.at[index, 'TEL2'] = tel_info_2 if tel_info_2 is not None else pd.NA
            df_numbers_clean.at[index, 'TEL3'] = tel_info_3 if tel_info_3 is not None else pd.NA
            df_numbers_clean.at[index, 'CP1'] = cp_1 if cp_1 is not None else pd.NA
            df_numbers_clean.at[index, 'CP2'] = cp_2 if cp_2 is not None else pd.NA
            df_numbers_clean.at[index, 'CP3'] = cp_3 if cp_3 is not None else pd.NA
        else:
            df_numbers_clean.drop(index, inplace=True)
        
    df_numbers_clean = df_numbers_clean.drop(columns=['TELEFONOS']) 
    
    return df_numbers_clean, total_tel_fijos, total_tel_moviles, total_tel_con_whatsapp, total_tel_sin_whatsapp, total_tel_no_contactable

def separar_nombres(
    campaña: str,
    df_numbers: pd.DataFrame, 
    df_nombres: pd.DataFrame, 
    col_id_credit:str, 
    col_name: str
):
    c = 0
    
    my_bar = st.progress(0, text="Separando nombres")
    
    for index, row in df_numbers.iterrows():
        c += 1
        
        my_bar.progress(value=c/len(df_numbers), text="Separando nombres")
        
        id_credit = row[col_id_credit]
        nombre_completo = df_nombres[df_nombres[col_id_credit] == id_credit][col_name].values[0]
        
        if nombre_completo is None:
            continue
        
        if (campaña == 'Invex'):
            nombre_completo = nombre_completo.replace("*", " ").replace("/", " ")
        elif (campaña == 'Tala'):
            nombre_completo = nombre_completo.replace("√°","á").replace("√©","é").replace("√≠","í").replace("√±","ñ").replace("√≥","ó").replace("√∫","ú").replace("√Å", "Á").replace("√Ö","ú")
            nombre_completo = nombre_completo.title()
        
        nombre_completo = nombre_completo.replace("  ", " ")
        
        palabras = nombre_completo.split()
    
        if len(palabras) == 1:
            nombre = palabras[0]
            nombre1 = ''
            apellido = ''
            apellido1 = ''
        elif len(palabras) == 2:
            nombre = palabras[0]
            nombre1 = ''
            apellido = palabras[1]
            apellido1 = ''
        else:
            nombre = palabras[0]
            nombre1 = ' '.join(palabras[1:-2])
            apellido = palabras[-2]
            apellido1 = palabras[-1]
        
        df_numbers.at[index, 'Nombre'] = nombre.capitalize()
        df_numbers.at[index, 'Nombre1'] = nombre1.capitalize()
        df_numbers.at[index, 'Apellido'] = apellido.capitalize()
        df_numbers.at[index, 'Apellido1'] = apellido1.capitalize()
        
    if col_name in df_numbers:
        df_numbers = df_numbers.drop(columns=[col_name])
        
    return df_numbers

def set_age_to_client(
    df_numbers: pd.DataFrame, 
    df_RFC: pd.DataFrame,
    col_id_credit: str,
    rfc_column: str,
):
    c = 0
    my_bar = st.progress(0, text="Obteniendo edades")
    
    for index, row in df_numbers.iterrows():
        c += 1
        my_bar.progress(value=c/len(df_numbers), text="Obteniendo edades")
        
        id_credit = row[col_id_credit]
        rfc = df_RFC[df_RFC[col_id_credit] == id_credit][rfc_column].values[0]
        
        if rfc is None:
            continue
        
        df_numbers.at[index, 'RFC'] = rfc
        
        df_numbers.at[index, 'FECHA_NACIMIENTO'] = rfc[4:10]
        df_numbers.at[index, 'FECHA_NACIMIENTO'] = pd.to_datetime(df_numbers.at[index, 'FECHA_NACIMIENTO'], format='%y%m%d', errors='coerce')
        
        today = datetime.now()
        df_numbers.at[index, 'EDAD'] = (today - df_numbers.at[index, 'FECHA_NACIMIENTO']).days // 365
        
        df_numbers.at[index, 'EDAD'] = df_numbers.at[index, 'EDAD'].astype(int)
        
        df_numbers.at[index, 'FECHA_NACIMIENTO'] = df_numbers.at[index, 'FECHA_NACIMIENTO'].strftime('%Y-%m-%d')
        
        if df_numbers.at[index, 'FECHA_NACIMIENTO'] is None:
            df_numbers.at[index, 'EDAD'] = None
        
    return df_numbers

def get_email_from_client(
    df_numbers: pd.DataFrame,
    df_email: pd.DataFrame,
    col_id_credit: str,
    email_column: str,
):
    c = 0
    my_bar = st.progress(0, text="Obteniendo correos electrónicos")
    
    for index, row in df_numbers.iterrows():
        c += 1
        my_bar.progress(value=c/len(df_numbers), text="Obteniendo correos electrónicos")
        
        id_credit = row[col_id_credit]
        email = df_email[df_email[col_id_credit] == id_credit][email_column].values[0]
        
        if email is None:
            continue
        
        if email_column not in df_numbers:
            df_numbers.at[index, email_column] = email
        
    return df_numbers

def generar_detonaciones(
    campaña: str,
    col_id_credit: str,
    df_numbers: pd.DataFrame = None,
    cols_tel: list = [],
    need_whatsapp_validation: bool = False,
    need_demographic_validation: bool = False,
    df_demo: pd.DataFrame = None,
    col_cp: str = None,
    need_rfc_validation: bool = False,
    df_RFC: pd.DataFrame = None,
    col_RFC: str = None,
    need_separete_names: bool = False,
    df_nombres: pd.DataFrame = None,
    col_nombres: str = None,
    need_email_validation: bool = False,
    df_email: pd.DataFrame = None,
    col_email: str = None,
):
    #! Inicio del proceso
    hora_inicio = time.strftime("%H:%M:%S")
    
    #!!!! SOLO ESTOY TOMANDO 10 REGISTROS PARA PRUEBAS
    df_numbers = df_numbers.head(10)
    
    #! Obtener total de números antes de eliminar duplicados
    total_tel_ori = str(df_numbers.shape[0] * len(cols_tel))
    
    #! Obtener el total de duplicados
    duplicados = df_numbers[df_numbers.duplicated(subset=[col_id_credit], keep=False)]
    df_duplicados = duplicados.groupby(col_id_credit).size().reset_index(name='counts')
    total_duplicados = str(df_duplicados.shape[0])
    
    #! Eliminar duplicados
    df_numbers = df_numbers.set_index(col_id_credit)
    df_numbers = df_numbers[~df_numbers.index.duplicated()]
    df_numbers = df_numbers.reset_index()
    
    #! Limpiar números telefónicos
    df_numbers = clean_phone_numbers(
        df_numbers=df_numbers,
        cols_tels=cols_tel,
        col_id_credit=col_id_credit,
        cp_column=col_cp,
        df_demografico=df_demo,
        need_demographic_validation=need_demographic_validation,
        need_whatsapp_validation=need_whatsapp_validation
    )
    
    #! Separar bombres
    if need_separete_names and df_nombres is not None and col_nombres is not None and col_nombres in df_nombres:
        df_numbers = separar_nombres(
            campaña=campaña,
            df_numbers=df_numbers,
            df_nombres=df_nombres,
            col_id_credit=col_id_credit,
            col_name=col_nombres,
        )
        
    #! Validar RFC
    if need_rfc_validation and df_RFC is not None and col_RFC is not None and col_RFC in df_RFC:
        df_numbers = set_age_to_client(
            df_numbers=df_numbers,
            col_id_credit=col_id_credit,
            df_RFC=df_RFC,
            rfc_column=col_RFC,
        )
    
    #! Limpiar correos electrónicos
    if need_email_validation and df_email is not None and col_email is not None and col_email in df_email:
        df_numbers = get_email_from_client(
            df_numbers=df_numbers,
            df_email=df_email,
            col_id_credit=col_id_credit,
            email_column=col_email,
        )
    
    
    #! Limpiar descuentos al formato numérico
    if 'descuento' in df_numbers:
        # Convertir descuentos a string
        df_numbers['descuento'] = df_numbers['descuento'].astype(str)
        df_numbers['descuento'] = pd.to_numeric(df_numbers['descuento'].str.rstrip('%'))
        df_numbers['descuento'] = (df_numbers['descuento'] * 100).astype(int)

    #! Obtener conteos totales de los tipos de números telefónicos
    df_contactable_numbers, total_tel_fijos, total_tel_moviles, total_tel_con_whatsapp, total_tel_sin_whatsapp, total_tel_no_contactable  = get_contactable_numbers(df_numbers)

    #! Generar archivo de detonaciones
    df_detonaciones = pd.DataFrame(columns=["Crédito","Tipo de Producto","Nombre","Nombre2","Apellido","Apellido2", "Fecha Apertura", "Meses vencidos", "Saldo capital", "Saldo Intereses", "Saldo TOTAL", "Descuento", "TEL1", "TEL2", "TEL3","CP1", "CP2", "CP3", "Correo", "Colonia", "Delegación", "Estado", "CP"])
    df_detonaciones["Crédito"] = df_contactable_numbers[col_id_credit]
    df_detonaciones["Nombre"] = df_contactable_numbers["Nombre"] if "Nombre" in df_contactable_numbers else np.nan
    df_detonaciones["Nombre2"] = df_contactable_numbers["Nombre2"] if "Nombre2" in df_contactable_numbers else np.nan
    df_detonaciones["Apellido"] = df_contactable_numbers["Apellido"] if "Apellido" in df_contactable_numbers else np.nan
    df_detonaciones["Apellido2"] = df_contactable_numbers["Apellido2"] if "Apellido2" in df_contactable_numbers else np.nan
    df_detonaciones["TEL1"] = df_contactable_numbers["TEL1"] if "TEL1" in df_contactable_numbers else np.nan
    df_detonaciones["TEL2"] = df_contactable_numbers["TEL2"] if "TEL2" in df_contactable_numbers else np.nan
    df_detonaciones["TEL3"] = df_contactable_numbers["TEL3"] if "TEL3" in df_contactable_numbers else np.nan
    df_detonaciones["CP1"] = df_contactable_numbers["CP1"] if "CP1" in df_contactable_numbers else np.nan
    df_detonaciones["CP2"] = df_contactable_numbers["CP2"] if "CP2" in df_contactable_numbers else np.nan
    df_detonaciones["CP3"] = df_contactable_numbers["CP3"] if "CP3" in df_contactable_numbers else np.nan
    df_detonaciones["Saldo TOTAL"] = df_contactable_numbers["Saldo total"] if "Saldo total" in df_contactable_numbers else np.nan
    df_detonaciones["Descuento"] = df_contactable_numbers["descuento"] if "descuento" in df_contactable_numbers else np.nan
    df_detonaciones["Meses vencidos"] = df_contactable_numbers["Meses vencidos"] if "Meses vencidos" in df_contactable_numbers else np.nan
    df_detonaciones["Tipo de Producto"] = df_contactable_numbers["producto"] if "producto" in df_contactable_numbers else np.nan
    df_detonaciones["Correo"] = df_contactable_numbers[col_email] if col_email in df_contactable_numbers else np.nan
    df_detonaciones["CP"] = df_contactable_numbers[col_cp] if col_cp in df_contactable_numbers else np.nan
    
    # Guardar el archivo de detonaciones
    #df_detonaciones.to_excel('../../Output/detonaciones.xlsx', index=False)

    df_detonaciones["Saldo TOTAL"] = df_detonaciones["Saldo TOTAL"].fillna(0).astype(float)
    df_detonaciones["Saldo TOTAL"] = df_detonaciones["Saldo TOTAL"].astype(float)
    df_detonaciones["Meses vencidos"] = df_detonaciones["Meses vencidos"].fillna(0).astype(int)
    df_detonaciones["Meses vencidos"] = df_detonaciones["Meses vencidos"].astype(int)
    
    monto_base_total = 0
    
    if "Saldo total" in df_numbers:
        df_numbers["Saldo total"] = df_numbers["Saldo total"].astype(float)
        monto_base_total = round(df_numbers["Saldo total"].sum(), 2)
        monto_base_total = f"${monto_base_total:,.2f}"

    #! Monto base contactable
    monto_base_contactable = round(df_detonaciones["Saldo TOTAL"].sum(), 2)
    monto_base_contactable = f"${monto_base_contactable:,.2f}"
    
    #! Mora contactable
    df_mora_contactable = df_detonaciones.groupby('Meses vencidos')['Saldo TOTAL'].agg(['count', 'sum']).reset_index()
    
    #! Moras por meses
    m1_sum = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 1]['sum'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 1].empty else 0
    m1_count = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 1]['count'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 1].empty else 0
    m1_str = f"{m1_count} - ${m1_sum:,.2f}"
    m2_sum = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 2]['sum'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 2].empty else 0
    m2_count = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 2]['count'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 2].empty else 0
    m2_str = f"{m2_count} - ${m2_sum:,.2f}"
    m3_sum = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 3]['sum'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 3].empty else 0
    m3_count = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 3]['count'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 3].empty else 0
    m3_str = f"{m3_count} - ${m3_sum:,.2f}"
    m4_sum = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 4]['sum'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 4].empty else 0
    m4_count = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 4]['count'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 4].empty else 0
    m4_str = f"{m4_count} - ${m4_sum:,.2f}"
    m5_sum = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 5]['sum'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 5].empty else 0
    m5_count = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 5]['count'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 5].empty else 0
    m5_str = f"{m5_count} - ${m5_sum:,.2f}"
    m6_sum = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 6]['sum'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 6].empty else 0
    m6_count = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 6]['count'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 6].empty else 0
    m6_str = f"{m6_count} - ${m6_sum:,.2f}"
    m7_sum = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 7]['sum'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 7].empty else 0
    m7_count = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 7]['count'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 7].empty else 0
    m7_str = f"{m7_count} - ${m7_sum:,.2f}"
    m8_sum = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 8]['sum'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 8].empty else 0
    m8_count = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 8]['count'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 8].empty else 0
    m8_str = f"{m8_count} - ${m8_sum:,.2f}"
    m9_sum = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 9]['sum'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 9].empty else 0
    m9_count = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 9]['count'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 9].empty else 0
    m9_str = f"{m9_count} - ${m9_sum:,.2f}"
    m10_sum = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 10]['sum'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 10].empty else 0
    m10_count = df_mora_contactable[df_mora_contactable['Meses vencidos'] == 10]['count'].values[0] if not df_mora_contactable[df_mora_contactable['Meses vencidos'] == 10].empty else 0
    m10_str = f"{m10_count} - ${m10_sum:,.2f}"
    m10plus_sum = df_mora_contactable[df_mora_contactable['Meses vencidos'] > 10]['sum'].sum().round(2)
    m10plus_count = df_mora_contactable[df_mora_contactable['Meses vencidos'] > 10]['count'].sum()
    m10plus_str = f"{m10plus_count} - ${m10plus_sum:,.2f}"
        
    #! Fin del proceso
    fecha_campaña = "2024-03-13"
    fecha_limpieza = time.strftime("%Y-%m-%d")
    hora_limpieza = time.strftime("%H:%M:%S")
    hora_final = time.strftime("%H:%M:%S")
    tiempo_procesamiento = datetime.strptime(hora_final, "%H:%M:%S") - datetime.strptime(hora_inicio, "%H:%M:%S")
   
    #! Insertar los datos en la tabla 'cleaning_history'
    conn = getDBConnection()
    cursor = conn.cursor()

    query = "INSERT INTO Cleaning_history (Campaña, Fecha_campaña, Fecha_limpieza, Hora_limpieza, Tiempo_procesamiento, Usuarios_duplicados, Numeros_tel_baseOri, Numeros_tel_fijos, Numeros_tel_moviles, Numeros_tel_moviles_sin_WA, Numeros_tel_moviles_WA, No_operables, Monto_base_contactable, M1, M2, M3, M4, M5, M6, M7, M8, M9, M10, M10plus) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    values = (campaña, fecha_campaña, fecha_limpieza, hora_limpieza, tiempo_procesamiento, total_duplicados, total_tel_ori, total_tel_fijos, total_tel_moviles, total_tel_sin_whatsapp, total_tel_con_whatsapp, total_tel_no_contactable, monto_base_contactable, m1_str, m2_str, m3_str, m4_str, m5_str, m6_str, m7_str, m8_str, m9_str, m10_str, m10plus_str,)
    
    cursor.execute(query, values)
    conn.commit()
    conn.close()
   
    # LISTO
    print("Datos insertados correctamente")
    
    return df_detonaciones