
import pandas as pd
import re

def clean_text(texto):
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
    df.columns = [clean_text(col) for col in df.columns]
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