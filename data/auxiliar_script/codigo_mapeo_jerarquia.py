# -*- coding: utf-8 -*-
"""
Created on Fri Nov 15 10:29:47 2024

@author: Ana Borrego
"""
import requests
import pandas as pd
import re
import src.main  as ahd

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

def remove_column(df, column_name):
    """
    Elimina una columna específica de un DataFrame.
    
    Parámetros:
        df (pd.DataFrame): El DataFrame del cual se eliminará la columna.
        column_name (str): El nombre de la columna a eliminar.
    
    Retorna:
        pd.DataFrame: Una copia del DataFrame sin la columna especificada.
    """
    if column_name in df.columns:
        return df.drop(columns=[column_name])
    else:
        print(f"La columna '{column_name}' no existe en el DataFrame.")
        return df

def union_by_cod_combination(df1_data, df2_hier, col1, col2):
    """
    Une dos DataFrames utilizando dos columnas que contienen códigos jerárquicos.

    Esta función prepara las columnas de los DataFrames para que sean compatibles en el proceso de unión,
    realiza la unión y luego limpia el resultado eliminando columnas no necesarias.

    Parámetros:
        df1_data (pd.DataFrame): Primer DataFrame, que contiene los datos principales.
        df2_hier (pd.DataFrame): Segundo DataFrame, que contiene la información jerárquica.
        col1 (str): Nombre de la columna del primer DataFrame que será usada para la unión.
        col2 (str): Nombre de la columna del segundo DataFrame que será usada para la unión.

    Retorna:
        pd.DataFrame: El DataFrame resultante después de la unión y limpieza de columnas.

    Funcionalidad:
        1. Preprocesa las columnas de los DataFrames (`col1` y `col2`) para asegurarse
           de que los valores sean cadenas compatibles con el proceso de unión.
           Si los valores son listas, se convierten en una cadena separada por comas.
        2. Realiza la unión de los DataFrames utilizando un `merge` con la columna correspondiente.
        3. Elimina las columnas que están completamente vacías (`NaN` o `None`).
        4. Remueve columnas específicas como el código de jerarquía (`alias+"_cod"`) y la columna común de combinación (`COD_combination`).

    Ejemplo de uso:
        >>> df1 = pd.DataFrame({
        ...     "Region_cod": [["00", "RA"], ["00", "RE"]],
        ...     "Value": [100, 200]
        ... })
        >>> df2 = pd.DataFrame({
        ...     "COD_combination": [["00", "RA"], ["00", "RE"]],
        ...     "Description": ["Region A", "Region B"]
        ... })
        >>> result = union_by_cod_combination(df1, df2, "Region_cod", "COD_combination")
        >>> print(result)
           Value Description
        0    100   Region A
        1    200   Region B

    Notas:
        - Para evitar problemas de modificaciones no deseadas en los DataFrames originales,
          se recomienda pasar copias de los mismos o asegurarse de que las modificaciones son esperadas.
        - Requiere que las columnas especificadas existan en ambos DataFrames.
    """
    # Procesamos las columnas que van a hacer la unión para que python las entienda y pueda encontrar sus coincidencias.
    for df, col in zip([df1_data, df2_hier], [col1, col2]):
        df[col] = df[col].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
    result = pd.merge(df1_data, df2_hier, left_on = col1, right_on= col2, how = 'left')
    # Eliminar las columnas que no tienen valores porque la jerarquía 
    # no utiliza todas las desagregaciones en el dato de consulta
    result = result.dropna(axis = 1, how = "all")
    # df de datos sin la columna de codificación correspondiente
    result = remove_column(result, alias+"_cod")
    # Borramos las columnas comunes cuando se unen todas las jerarquías "COD_COMBINATION"
    result = remove_column(result, "COD_combination")
    return result

def process_measures_columns(df, measures):
    """
    Procesa columnas que contienen diccionarios, extrayendo el valor asociado a la clave 'val', 
    y reorganiza dichas columnas para que aparezcan al final del DataFrame.

    Parámetros:
        df (pd.DataFrame): El DataFrame que será modificado.
        measures (list): Lista de nombres de columnas que contienen diccionarios con la clave 'val'.
                         Estas columnas serán procesadas y reordenadas al final del DataFrame.

    Retorna:
        pd.DataFrame: El DataFrame modificado, con las columnas `measures` procesadas y reubicadas 
                      al final de la estructura.

    Funcionalidad:
        1. Recorre cada columna especificada en `measures`.
        2. Si la columna existe en el DataFrame, transforma sus valores:
           - Si el valor es un diccionario, extrae el valor asociado a la clave `'val'`.
           - Si no es un diccionario, mantiene el valor original.
        3. Reorganiza las columnas del DataFrame para que las incluidas en `measures` aparezcan al final.

    Ejemplo de uso:
        >>> data = {
        ...     "Nombre": ["A", "B", "C"],
        ...     "Edad": [25, 30, 22],
        ...     "Medida1": [{"val": 100, "format": "100"}, {"val": 200, "format": "200"}, {"val": 300, "format": "300"}],
        ...     "Medida2": [{"val": 10, "format": "10"}, {"val": 20, "format": "20"}, {"val": 30, "format": "30"}]
        ... }
        >>> df = pd.DataFrame(data)
        >>> measures = ["Medida1", "Medida2"]
        >>> result = process_measures_columns(df, measures)
        >>> print(result)
          Nombre  Edad  Medida1  Medida2
        0      A    25      100       10
        1      B    30      200       20
        2      C    22      300       30

    Notas:
        - Asegúrate de que las columnas en `measures` existan en el DataFrame, ya que las columnas faltantes serán ignoradas.
        - Si las columnas especificadas no contienen diccionarios, sus valores no serán modificados.
        - La función realiza una reorganización de columnas; esto puede afectar procesos que dependan del orden original de las mismas.
    """
    for col in measures:
        if col in df.columns:
            # Extraer el valor de la clave 'val' en cada fila
            df[col] = df[col].apply(lambda x: x.get('val') if isinstance(x, dict) else x)
    
    # Reorganizar las columnas: mover las columnas de `measures` al final
    remaining_columns = [col for col in df.columns if col not in measures]
    df = df[remaining_columns + measures]
    
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

# handler = ahd.APIDataHandler(response)
handler = ahd.PIDataHandler(response)
dataset = handler.get_DataFrame_dataJSON()
processed_data = handler.process_all_hierarchies()

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
    # Nos quedamos sólo con el código porque no nos interesa realmente la microinformación
    # con la que venían inicialmente los datos, le asignaremos la categoría correspondiente según su jerarquía. 
cod_columns = dataset.filter(regex='_cod$').columns.tolist()
selected_columns = cod_columns + col_medida
cod_df = dataset[selected_columns].copy()

# esas columnas de codificación de jerarquía serán mapeadas según la jerarquía con la tabla `processed_data`
for alias, col in zip(hierarchies_used, name_cols): 
    # Filtramos para los posibles valores de la jerarquía en cuestión: (importante hacer .copy())
    hier_df_values = processed_data[processed_data['Variable']==alias].copy()
        # Seleccionar columnas que no son 'Variable' ni 'id' # aportan información para la jerarquía pero no para la categorización.
    columns_to_keep = hier_df_values.columns.difference(['Variable', 'id'])
    hier_df_values = hier_df_values[columns_to_keep]
    
    # Seleccionamos las columnas de interés para unir al dataset de datos:
    # columns_to_select = hier_df_values.filter(regex=r'^COD_combination$|^Des\d+$').columns
    # to_merge = hier_df_values[columns_to_select]
    
    # Renombramos las columnas de descripción para ponerles el valor de columna y su nivel de desagregación
    # el cuál viene dado por la i en "Des{i}". 
    columns_to_rename = hier_df_values.filter(regex=r'^Des\d+$').columns
        # Crear un diccionario para renombrar las columnas, reemplazando 'Des' por el valor de col
    rename_dict = {col_name: col_name.replace('Des', col) for col_name in columns_to_rename}
        # Renombrar las columnas en el DataFrame
    hier_df_values = hier_df_values.rename(columns=rename_dict)
    
    # Unimos con el conjunto de datos por "cod_combination" en los valores de jerarquía y por la columna correspondiente "_cod" en el dataset
    cod_df = union_by_cod_combination(cod_df, hier_df_values, alias+"_cod", "COD_combination")

# Reorganización y final de la limpieza de los datos:
cod_df = process_measures_columns(cod_df, col_medida)