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
    Normaliza los nombres de las columnas de un DataFrame eliminando acentos
    y reemplazando los espacios por guiones bajos.
    
    Parámetros:
    df (pd.DataFrame): DataFrame con nombres de columnas a normalizar.
    
    Retorna:
    pd.DataFrame: DataFrame con nombres de columnas normalizados.
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

### Mapeo de Jerarquías

hierarchies_used = pd.unique(processed_data.Variable)
name_cols = [re.search(r'D(?:_AA)?_(.*?)_0', elem).group(1) for elem in hierarchies_used]

# =============================================================================
# Explicación de las expresiones regulares
# D(?:_AA)?_ : busca el prefijo D_, opcionalmente seguido de _AA_.
# (.*?) : grupo de captura captura cualquier palabra entre el prefijo y _0.
# _0 : es el sufijo final que indica el final de la palabra que necesitas.
# =============================================================================
# Explicación de las funciones utilizadas para el manejo de expresiones regulares:
# 1. re.search(pattern, string) : buscar pattern en una cadena de texto string. 
#       Busca en cualquier posición, a diferencia de re.match() que busca al inicio de la cadena. 
#       Devuelve un objeto de coincidencia si encuentra el patrón en la cadena; de lo contrario, devuelve None
# Cuando re.search() encuentra una coincidencia, devuelve un objeto de coincidencia (match_object).
# 2. match_object.group(1) 
#       Usamos .group(1) para extraer específicamente la parte de la coincidencia que está dentro de los paréntesis
#       (...) en el patrón de expresión regular. 
#       En nuestro caso, el grupo de captura es (.*?) y con .group(1), estamos indicando que queremos el contenido de este primer grupo de captura.
# =============================================================================
# =============================================================================
# Ejemplo: Supón que tienes una cadena como "D_AA_TERRITROIO_0". Con re.search(), el objeto de coincidencia tendrá:
#           Grupo 0 (toda la coincidencia): "D_AA_TERRITROIO_0"
#           Grupo 1 (solo el grupo capturado): "TERRITROIO"
# Así, .group(1) devuelve "TERRITROIO", que es el valor que queremos extraer de la cadena.
# =============================================================================

dataset = norm_columns_name(dataset)
n_med = len(handler.measures)
if n_med == 1: 
    col_medida = [limpiar_texto(handler.measures[0]["des"])]
else: 
    col_medida = [limpiar_texto(handler.measures[i]["des"]) for i in range(n_med)]

# columnas que contienen el código : 
cod_columns = dataset.filter(regex='_cod$').columns.tolist()
selected_columns = cod_columns + col_medida
cod_df = dataset[selected_columns]

# esas columnas serán mapeadas según la jerarquía con la tabla `processed_data`
for alias, col in zip(hierarchies_used, name_cols): 
    # Filtramos para los posibles valores de la jerarquía en cuestión:
    hier_df_values = processed_data[processed_data['Variable']==alias]
    # Seleccionamos las columnas de interés para unir al dataset de datos:
    columns_to_select = hier_df_values.filter(regex=r'^COD_combination$|^Des\d+$').columns
    to_merge = hier_df_values[columns_to_select]
    # Renombramos las columnas de descripción para ponerles el valor de columna y su nivel de desagregación
    # el cuál viene dado por la i en "Des{i}". 
    columns_to_rename = hier_df_values.filter(regex=r'^Des\d+$').columns
        # Crear un diccionario para renombrar las columnas, reemplazando 'Des' por el valor de col
    rename_dict = {col_name: col_name.replace('Des', col) for col_name in columns_to_rename}
        # Renombrar las columnas en el DataFrame
    hier_df_values = hier_df_values.rename(columns=rename_dict)
    # Unimos con el conjunto de datos por "cod_combination" en los valores de jerarquía y por la columna correspondiente "_cod" en el dataset
    dataset = pd.merge(dataset, hier_df_values, left_on = alias + "_cod", right_on = "COD_combination", how = "left")

    
