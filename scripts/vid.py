import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm
import warnings
import sys
import time
warnings.filterwarnings("ignore")
import pandas as pd
from unidecode import unidecode
import mysql.connector
import requests
from datetime import datetime

doc_IFT = "C:/Users/carlo/Desktop/dashboard_clean/documents/df_ift.csv"
doc_SEPOMEX = "C:/Users/carlo/Desktop/dashboard_clean/documents/df_sepomex.txt"

equivalencias_txt = "AGS|Aguascalientes|BC|Baja California|BCS|Baja California Sur|CAMP|Campeche|CDMX|Ciudad de México|CHIH|Chihuahua|CHIS|Chiapas|COAH|Coahuila de Zaragoza|COL|Colima|DGO|Durango|ERROR|Error|GRO|Guerrero|GTO|Guanajuato|HGO|Hidalgo|JAL|Jalisco|MEX|México|MICH|Michoacán de Ocampo|MOR|Morelos|NAY|Nayarit|NL|Nuevo León|OAX|Oaxaca|PUE|Puebla|QRO|Querétaro|QROO|Quintana Roo|SIN|Sinaloa|SLP|San Luis Potosí|SON|Sonora|TAB|Tabasco|TAMPS|Tamaulipas|TLAX|Tlaxcala|VER|Veracruz de Ignacio de la Llave|YUC|Yucatán|ZAC|Zacatecas"
equivalencias_list = equivalencias_txt.split('|')
equivalencias_dict = {equivalencias_list[i + 1]: equivalencias_list[i] for i in range(0, len(equivalencias_list), 2)}

df_IFT = pd.read_csv(doc_IFT)
df_IFT['NIR_SERIE'] = df_IFT['NIR_SERIE'].astype(str)

columns = ['d_codigo', 'd_asenta', 'd_tipo_asenta', 'D_mnpio', 'd_estado', 'd_ciudad', 'd_CP', 'c_estado', 'c_oficina', 'c_CP', 'c_tipo_asenta', 'c_mnpio', 'id_asenta_cpcons', 'd_zona', 'c_cve_ciudad']
df_sepomex = pd.read_csv(doc_SEPOMEX, sep="|", header=0, names=columns, encoding='ISO-8859-1')

def getDBConnection():
     # Configuración de la conexión a la base de datos
    conn = mysql.connector.connect(
        host='pernexium-db.cfioetbrvik6.us-east-2.rds.amazonaws.com',
        user='analitica_alain',
        password="gBi1{H120U3DT@c",
        database='analitica',
    )
    return conn

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

def clean_phone_numbers(
    df_numbers: pd.DataFrame, 
    need_demographic_validation: bool, 
    need_whatsapp_validation: bool,
    cols_tels: list,  
    cp_column: None,
    ):
    
    df_numbers['TELEFONOS'] = None
             
    c = 0
    
    for col in cols_tels:
        if df_numbers[col].dtype == np.float64:
            df_numbers[col] = df_numbers[col].apply(lambda x: x if pd.isnull(x) else str(int(x)))

    # Iterrar los números telefónicos
    for index, row in df_numbers.iterrows():
        c += 1
        tels = []
        cp = None
        processed_numbers = {}
        
        # Mostrar el progreso
        if need_demographic_validation:
            sys.stdout.write(f'\rValidando números con información demográfica ({c/len(df_numbers)*100:.2f}%)')
            sys.stdout.flush()
        else:
            sys.stdout.write(f'\rValidando números ({c/len(df_numbers)*100:.2f}%)')
            sys.stdout.flush()
                
        
        # Obtener el código postal del DataFrame de la información demográfica
        if need_demographic_validation and cp_column is not None and cp_column in df_numbers:
            try:
                cp = df_numbers.at[index, cp_column]
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
    
    for index, row in df_numbers_clean.iterrows():
        tels = row['TELEFONOS']
        
        tel_info = None
        
        for tel in tels:
            if tel_info is None and tel['TIPO_RED'] == 'MOVIL' and tel['WHATSAPP'] == True:
                tel_info = tel
            
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
        
        if tel_info is not None:
            df_numbers_clean.at[index, 'TELEFONO'] = tel_info['TELEFONO'] 
            df_numbers_clean.at[index, 'TIPO_RED'] = tel_info['TIPO_RED']
            df_numbers_clean.at[index, 'POBLACION'] = tel_info['POBLACION']
            df_numbers_clean.at[index, 'MUNICIPIO'] = tel_info['MUNICIPIO']
            df_numbers_clean.at[index, 'ESTADO'] = tel_info['ESTADO']
            df_numbers_clean.at[index, 'ESTADO SEPOMEX'] = tel_info['ESTADO SEPOMEX']
            df_numbers_clean.at[index, 'ESTADO ABREVIATURA'] = tel_info['ESTADO ABREVIATURA']
            df_numbers_clean.at[index, 'EQUIVALENTE CORRECTO'] = tel_info['EQUIVALENTE CORRECTO']
            df_numbers_clean.at[index, 'RAZON_SOCIAL'] = tel_info['RAZON_SOCIAL']
            df_numbers_clean.at[index, 'WHATSAPP'] = tel_info['WHATSAPP']
        else:
            df_numbers_clean.drop(index, inplace=True)
        
    df_numbers_clean = df_numbers_clean.drop(columns=['TELEFONOS']) 
    
    return df_numbers_clean, total_tel_fijos, total_tel_moviles, total_tel_con_whatsapp, total_tel_sin_whatsapp, total_tel_no_contactable
        
def separar_nombres(nombre_completo):
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
    return nombre, nombre1, apellido, apellido1

def generar_detonaciones(
    df_numbers: pd, 
    col_id_client: str,
    fecha: str, 
    hora_inicio: str,
    total_tel_ori: int, 
    campaña: str,
    total_duplicados: int,
):
    #! Limpiar descuentos al formato numérico
    if 'descuento' in df_numbers:
        df_numbers['descuento'] = pd.to_numeric(df_numbers['descuento'].str.rstrip('%'))
        df_numbers['descuento'] = (df_numbers['descuento'] * 100).astype(int)

    #! Obtener conteos totales de los tipos de números telefónicos
    df_contactable_numbers, total_tel_fijos, total_tel_moviles, total_tel_con_whatsapp, total_tel_sin_whatsapp, total_tel_no_contactable  = get_contactable_numbers(df_phone_numbers_clean)

    #! Generar archivo de detonaciones
    df_detonaciones = pd.DataFrame(columns=["Crédito","Tipo de Producto","Nombre","Nombre2","Apellido","Apellido2", "Fecha Apertura", "Meses vencidos", "Saldo capital", "Saldo Intereses", "Saldo TOTAL", "Descuento", "TEL1", "TEL2", "TEL3", "TEL4", "Correo", "Colonia", "Delegación", "Estado", "CP"])
    df_detonaciones["Crédito"] = df_contactable_numbers[col_id_client]
    df_detonaciones["Nombre"] = df_contactable_numbers["Nombre"] if "Nombre" in df_contactable_numbers else np.nan
    df_detonaciones["Nombre2"] = df_contactable_numbers["Nombre2"] if "Nombre2" in df_contactable_numbers else np.nan
    df_detonaciones["Apellido"] = df_contactable_numbers["Apellido"] if "Apellido" in df_contactable_numbers else np.nan
    df_detonaciones["Apellido2"] = df_contactable_numbers["Apellido2"] if "Apellido2" in df_contactable_numbers else np.nan
    df_detonaciones["TEL1"] = df_contactable_numbers['TELEFONO']
    df_detonaciones["Saldo TOTAL"] = df_contactable_numbers["Saldo total"] if "Saldo total" in df_contactable_numbers else np.nan
    df_detonaciones["Descuento"] = df_contactable_numbers["descuento"] if "descuento" in df_contactable_numbers else np.nan
    df_detonaciones["Meses vencidos"] = df_contactable_numbers["Meses vencidos"] if "Meses vencidos" in df_contactable_numbers else np.nan
    df_detonaciones["Tipo de Producto"] = df_contactable_numbers["producto"] if "producto" in df_contactable_numbers else np.nan
    
    # Guardar el archivo de detonaciones
    #df_detonaciones.to_excel('../../Output/detonaciones.xlsx', index=False)
    
    # Convertir el salto total a float y meses a enteros
    # si el valor es nan se convierte a 0
    df_detonaciones["Saldo TOTAL"] = df_detonaciones["Saldo TOTAL"].fillna(0).astype(float)
    df_detonaciones["Saldo TOTAL"] = df_detonaciones["Saldo TOTAL"].astype(float)
    df_detonaciones["Meses vencidos"] = df_detonaciones["Meses vencidos"].fillna(0).astype(int)
    df_detonaciones["Meses vencidos"] = df_detonaciones["Meses vencidos"].astype(int)
    
    monto_base_total = 0
    
    if "Saldo total" in df_numbers:
        df_numbers["Saldo total"] = df_numbers["Saldo total"].astype(float)
        monto_base_total = round(df_numbers["Saldo total"].sum(), 2)
        monto_base_total = f"${monto_base_total:,.2f}"

    # Monto base contactable
    monto_base_contactable = round(df_detonaciones["Saldo TOTAL"].sum(), 2)
    monto_base_contactable = f"${monto_base_contactable:,.2f}"
    
    # Mora contactable
    df_mora_contactable = df_detonaciones.groupby('Meses vencidos')['Saldo TOTAL'].agg(['count', 'sum']).reset_index()
    
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
        
    # Fin del proceso
    fecha_campaña = fecha
    fecha_limpieza = time.strftime("%Y-%m-%d")
    hora_limpieza = time.strftime("%H:%M:%S")
    hora_final = time.strftime("%H:%M:%S")
    tiempo_procesamiento = datetime.strptime(hora_final, "%H:%M:%S") - datetime.strptime(hora_inicio, "%H:%M:%S")
   
    # Insertar los datos en la tabla 'cleaning_history'
    conn = getDBConnection()
    cursor = conn.cursor()

    query = "INSERT INTO Cleaning_history (Campaña, Fecha_campaña, Fecha_limpieza, Hora_limpieza, Tiempo_procesamiento, Usuarios_duplicados, Numeros_tel_baseOri, Numeros_tel_fijos, Numeros_tel_moviles, Numeros_tel_moviles_sin_WA, Numeros_tel_moviles_WA, No_operables, Monto_base_contactable, M1, M2, M3, M4, M5, M6, M7, M8, M9, M10, M10plus) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    values = (campaña, fecha_campaña, fecha_limpieza, hora_limpieza, tiempo_procesamiento, total_duplicados, total_tel_ori, total_tel_fijos, total_tel_moviles, total_tel_sin_whatsapp, total_tel_con_whatsapp, total_tel_no_contactable, monto_base_contactable, m1_str, m2_str, m3_str, m4_str, m5_str, m6_str, m7_str, m8_str, m9_str, m10_str, m10plus_str,)
    
    cursor.execute(query, values)
    conn.commit()
    conn.close()
   
    # LISTO
    print("Datos insertados correctamente")
    
def merge_databases(
    databases: list[pd.DataFrame],
    col_id_client: str,
):
    list_of_df_with_index = []
    
    for df in databases:
        df_clean = df.set_index(col_id_client)
        list_of_df_with_index.append(df_clean)
        
    df_merged = pd.concat(list_of_df_with_index, axis=1, join='outer')
    df_merged = df_merged.reset_index()
    
    return df_merged

# TODO: AJUSTAR AL DASHBOARD
# def group_products_by_client(
#     df_merged: pd.DataFrame, 
#     col_id_client: str, 
#     cols_productos: list
# ):    
#     # Creamos un diccionario para almacenar los productos de cada cliente
#     products_by_client = {}
#     c = 0

#     for _, row in df_merged.iterrows():
#         c += 1
#         sys.stdout.write(f'\rAgrupando productos por cliente ({c/len(df_merged)*100:.2f}%)')
#         sys.stdout.flush()
#         # Obtenemos el identificador único del cliente
#         client_id = row[col_id_client]
        
#         # Creamos un diccionario para el producto actual
#         product_dict = {col: row[col] for col in cols_productos}
        
#         # Verificamos si ya hemos encontrado productos para este cliente
#         if client_id in products_by_client:
#             # Si el cliente ya existe en el diccionario, agregamos el producto a su lista existente
#             products_by_client[client_id].append(product_dict)
#         else:
#             # Si es un nuevo cliente, creamos una nueva lista de productos para él
#             products_by_client[client_id] = [product_dict]

#     # Creamos una lista de diccionarios para cada cliente
#     response = [{col_id_client: cliente_id, 'PRODUCTOS': products} for cliente_id, products in products_by_client.items()]

#     # Creamos el DataFrame a partir de la lista de diccionarios
#     df_products = pd.DataFrame(response)
    
#     # Usamos un index para poder hacer un merge con el DataFrame original
#     df_products.set_index(col_id_client, inplace=True)
#     df_merged.set_index(col_id_client, inplace=True)
    
#     # Hacemos un merge entre el DataFrame original y el de productos, eliminando duplicados y borrando las columnas de productos del DataFrame original
#     df_merged = df_merged[~df_merged.index.duplicated()]
#     df_merged = df_merged.drop(columns=cols_productos)
#     df_merged = pd.concat([df_merged, df_products], axis=1, join='outer')
#     df_merged = df_merged.reset_index()
    
#     return df_merged

def set_age_to_client(df_numbers_clean: pd.DataFrame, rfc_column):
    sys.stdout.write('\rValidando RFC')
    sys.stdout.flush()
    
    df_age_clean = df_numbers_clean.copy()
    
   # Verificar si la columna de RFC existe
    if rfc_column not in df_age_clean.columns:
        #print(f'La columna "{rfc_column}" no existe en df_age_clean.')
        return df_age_clean
    
    # Extraer la fecha de nacimiento del RFC
    df_age_clean['FECHA_NACIMIENTO'] = df_age_clean[rfc_column].str[4:10]
    
    # Convertir la fecha de nacimiento a objetos de fecha
    df_age_clean['FECHA_NACIMIENTO'] = pd.to_datetime(df_age_clean['FECHA_NACIMIENTO'], format='%y%m%d', errors='coerce')
    
    # Calcular la edad en años
    today = datetime.now()
    df_age_clean['EDAD'] = (today - df_age_clean['FECHA_NACIMIENTO']).dt.days // 365
    
    # Convertir la edad a entero
    df_age_clean['EDAD'] = df_age_clean['EDAD'].astype('Int64')
    
    # Convertir la fecha de nacimiento a string
    df_age_clean['FECHA_NACIMIENTO'] = df_age_clean['FECHA_NACIMIENTO'].dt.strftime('%Y-%m-%d')
    
    # Manejar casos de RFC vacío o en formato incorrecto
    df_age_clean.loc[df_age_clean['FECHA_NACIMIENTO'].isnull(), 'EDAD'] = None
    
    return df_age_clean
    
def clean_databases(
    campaña: str,
    databases: list[pd.DataFrame],
    need_demographic_validation: bool,
    need_whatsapp_validation: bool,
    need_separate_names: bool,
    need_group_products_by_client: bool,
    need_validate_RFC: bool,
    cols_tels: list[str],
    cols_productos: list[str],
    col_id_client: str,
    col_id_product: str,
    col_cp: None,
    col_RFC: None,
    col_name: None,
):
    # Eliminar None de databases
    databases = [df for df in databases if df is not None]
    df_merged = merge_databases(databases, col_id_client)
    df_merged = df_merged.head(100)
    
    #! Inicio del proceso
    hora_inicio = time.strftime("%H:%M:%S")
    
    #! Obtener total de números antes de eliminar duplicados
    total_tel_ori = str(df_merged.shape[0] * len(cols_tels))
    
    # Obtener el total de duplicados
    duplicados = df_merged[df_merged.duplicated(subset=[col_id_client], keep=False)]
    df_duplicados = duplicados.groupby(col_id_client).size().reset_index(name='counts')
    total_duplicados = str(df_duplicados.shape[0])
    
    #! Eliminar duplicados
    df_merged = df_merged.set_index(col_id_client)
    df_merged = df_merged[~df_merged.index.duplicated()]
    df_merged = df_merged.reset_index()
        
    #! Limpiar números telefónicos y hacer las validaciones correspondientes
    df_clean = clean_phone_numbers(
        df_numbers = df_merged,
        cols_tels = cols_tels,
        cp_column = col_cp,
        need_demographic_validation = need_demographic_validation,
        need_whatsapp_validation = need_whatsapp_validation,
    )
        
    #! Separar el nombre completo en caso de ser necesario
    if need_separate_names and col_name is not None and col_name in df_clean:
        if (campaña == 'Invex'):
            df_clean[col_name] = df_clean[col_name].str.replace("*", " ").str.replace("/", " ")
        elif (campaña == 'Tala'):
            df_clean[col_name] = df_clean[col_name].str.replace("√°","á").str.replace("√©","é").str.replace("√≠","í").str.replace("√±","ñ").str.replace("√≥","ó").str.replace("√∫","ú").str.replace("√Å", "Á").str.replace("√Ö","ú")
            df_clean[col_name] = df_clean[col_name].str.title()
        
        df_phone_numbers_clean[col_name] = df_phone_numbers_clean[col_name].str.replace("  ", " ")
        df_phone_numbers_clean[['Nombre', 'Nombre2', 'Apellido', 'Apellido2']] = df_phone_numbers_clean[col_name].apply(separar_nombres).apply(pd.Series)
        df_phone_numbers_clean = df_phone_numbers_clean.drop(columns=[col_name])
    
    #! Asignar la edad a los clientes si necesita validación de RFC
    if need_validate_RFC:
        df_clean = set_age_to_client(
            df_numbers_clean = df_clean,
            rfc_column = col_RFC,
        )
    
    df_final = df_clean.copy()
    df_final.to_json('../../Output/df_final.json', orient='records')
    
    # TODO: AGRUPACIÓN DE PRODUCTOS PENDIENTE
    #! Agrupar los productos por cliente si es necesario
    # if need_group_products_by_client:
    #     df_final = group_products_by_client(
    #         df_merged = df_final,
    #         col_id_client = col_id_client,
    #         cols_productos = cols_productos
    #     )
        
    #! Generar detonaciones
    generar_detonaciones(
        col_id_client = col_id_client,
        df_numbers = df_clean,
        hora_inicio = hora_inicio,
        total_duplicados = total_duplicados,
        total_tel_ori = total_tel_ori,
        fecha = time.strftime("%Y-%m-%d"),
        campaña = campaña,
    )