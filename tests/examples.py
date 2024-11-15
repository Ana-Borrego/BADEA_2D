# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 12:21:55 2024

@author: Ana Borrego
"""
import requests
import pandas as pd
import re
import class_APIHandlerData_complete  as ahd

# Función para reemplazar acentos y espacios usando regex
def limpiar_texto(texto):
    """
    Limpia un texto eliminando acentos y caracteres especiales, y reemplaza espacios por guiones bajos.

    Parámetros:
        texto (str): El texto a limpiar.

    Retorna:
        str: El texto limpio, sin acentos ni espacios.

    Funcionalidad:
        1. Reemplaza caracteres acentuados por sus equivalentes sin acento, 
           según un diccionario predefinido.
        2. Sustituye espacios (incluidos múltiples espacios consecutivos) por guiones bajos ('_').

    Ejemplo de uso:
        >>> limpiar_texto("Café con Leche")
        'Cafe_con_Leche'
        >>> limpiar_texto("Niño/a Ágil")
        'Nino_a_Agil'

    Notas:
        - Es útil para uniformizar texto antes de procesar datos.
        - Utiliza expresiones regulares para realizar las sustituciones.

    Dependencias:
        - La función usa el módulo `re` de Python para trabajar con expresiones regulares.
    """
    # Diccionario de reemplazo de acentos
    acentos = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U',
        'ñ': 'n', 'Ñ': 'N'
    }
    # Reemplazar caracteres acentuados
    for acento, reemplazo in acentos.items():
        texto = re.sub(acento, reemplazo, texto)
    # Reemplazar espacios por guiones bajos
    texto = re.sub(r'\s+', '_', texto)
    return texto

def norm_columns_name(df):
    """
    Normaliza los nombres de las columnas de un DataFrame eliminando acentos y reemplazando espacios por guiones bajos.

    Parámetros:
        df (pd.DataFrame): DataFrame cuyas columnas serán normalizadas.

    Retorna:
        pd.DataFrame: Una copia del DataFrame con nombres de columnas normalizados.

    Funcionalidad:
        1. Recorre los nombres de las columnas del DataFrame.
        2. Aplica la función `limpiar_texto` a cada nombre de columna
        3. Asigna los nombres normalizados al DataFrame original.

    Ejemplo de uso:
        >>> import pandas as pd
        >>> data = {"Número de Teléfono": [123, 456], "Ciudad": ["México", "Bogotá"]}
        >>> df = pd.DataFrame(data)
        >>> norm_columns_name(df)
           Numero_de_Telefono   Ciudad
        0                 123   México
        1                 456   Bogotá

    Notas:
        - Este proceso es útil para evitar problemas de compatibilidad al trabajar con columnas
          que tienen espacios o caracteres no estándar.
        - Asegúrate de que el DataFrame tenga columnas no vacías antes de usar esta función.

    Dependencias:
        - Requiere la función `limpiar_texto` para procesar cada nombre de columna.
    """
    # Renombrar columnas aplicando la función de limpieza
    df.columns = [limpiar_texto(col) for col in df.columns]
    return df


## Ejemplos de uso: ---------------------------------------------
    
url = "https://www.juntadeandalucia.es/institutodeestadisticaycartografia/intranet/admin/rest/v1.0/consulta/44804?"

# =============================================================================
# D_TEMPORAL_0=180194&D_AA_TERRITROIO_0=515892&D_SEXO_0=3691,3689,3690&posord=f[D_AA_TERRITROIO_0],f[D_TEMPORAL_0],f[D_SEXO_0],f[D_EDAD_0],f[D_AA_DURAULTEMPR_0],c[Measures]"
# =============================================================================

# parámetros de consulta: 
params = {
    "D_TEMPORAL_0" : "180194",
    "AA_TERRITROIO_0" : "515892",
    "D_SEXO_0" : "3691,3689,3690",
    "posord" : "f[D_AA_TERRITROIO_0],f[D_TEMPORAL_0],f[D_SEXO_0],f[D_EDAD_0],f[D_AA_DURAULTEMPR_0],c[Measures]"
}

# Realizar request GET
response = requests.get(url, params = params)

# =============================================================================
# 1. Crea una instancia de la clase APIHandler pasando la respuesta `response` de la API.
# =============================================================================

# handler = ahd.APIDataHandler(response)
handler = ahd.APIDataHandler(response)

# =============================================================================
# 1.1. Consulta de los atributos iniciales de la clase.
# =============================================================================
handler.response
handler.JSONdata
handler.hierarchies
handler.data
handler.measures
handler.metainfo
handler.id_consulta

# =============================================================================
# 2. Consulta de los elementos en formato diccionario. 
# =============================================================================

dict_response = handler.get_elements_of_response()
dict_response
dict_response["jerarquias"]
dict_response["id_consulta"]

# =============================================================================
# 3. Consulta de datos que devuelve BADEA tras la petición, en formato DataFrame:
# =============================================================================

dataset = handler.get_DataFrame_dataJSON()
handler.df_data

# =============================================================================
# 4. Hacer petición a "url" de información de una de las jerarquías. 
# =============================================================================

hier = handler.hierarchies[0]
values_hier = handler.request_hierarchies_values(hier)

# =============================================================================
# 5. Procesa las jerarquías llamando al método `process_all_hierarchies`
# =============================================================================

processed_data = handler.process_all_hierarchies()

# =============================================================================
# 5.1. Guardar el dataframe resultante con `.to_excel()`
# =============================================================================
# cleaned_data.to_excel("jerarquias_limpiadas.xlsx")
# =============================================================================
