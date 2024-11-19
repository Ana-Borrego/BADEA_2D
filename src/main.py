# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 11:20:04 2024

@author: Ana Borrego
"""

import requests
import pandas as pd
import logging
import re

# Auxiliar functions for processing data in APIHandlerData
import os
os.chdir("C:/Users/Ana Borrego/Desktop/proyectos/andalucia_emprende/VISOR/request_py/")
from src import functions

# Class for handle API response definition. 
class APIDataHandler:
    
    def __init__(self, response):
        """
        Constructor que toma la respuesta JSON de una consulta API y extrae sus elementos clave.
        
        Este constructor inicializa los atributos de la clase a partir de la respuesta JSON de una consulta API,
        extrayendo las jerarquías, los datos, las medidas y la metainformación de la respuesta. También configura
        un logger para registrar los eventos relacionados con esta clase.
    
        Parámetros:
            response (requests.Response): La respuesta de una consulta a una API, que se espera que esté en formato JSON.
        
        Retorna:
            Ninguno: Este método no retorna un valor. Inicializa los atributos de la clase.
        
        Funcionalidad:
            - El método extrae los siguientes elementos de la respuesta JSON:
              - `hierarchies`: Una lista de jerarquías en la respuesta.
              - `data`: Una lista de datos estructurados en la respuesta.
              - `measures`: Una lista de medidas relacionadas con los datos.
              - `metainfo`: Información adicional sobre la consulta.
              - `id_consulta`: Un identificador único para la consulta.
            - El logger se configura para registrar eventos específicos de esta clase, utilizando el `id_consulta` como parte del nombre del logger.
    
        Ejemplo de uso:
            >>> import requests
            >>> response = requests.get('https://api.example.com/data')
            >>> handler = APIDataHandler(response)
            >>> print(handler.hierarchies)
            [{'alias': 'Country'}, {'alias': 'City'}]
            >>> print(handler.data)
            [{'Country': 'USA', 'City': 'New York', 'Value': 100}]
        
        Notas:
            - Este constructor requiere que la respuesta de la API sea un objeto `requests.Response`, 
              y que su contenido esté en formato JSON con las claves `hierarchies`, `data`, `measures` y `metainfo`.
            - Si alguna de estas claves no está presente en la respuesta, se usará un valor predeterminado (como una lista vacía o un diccionario vacío).
            - El logger se configura dinámicamente en función del `id_consulta` para cada instancia de la clase.
        """
        self.response = response
        self.JSONdata = response.json().copy()

        # Extraer los elementos principales de la respuesta JSON
        self.hierarchies = self.JSONdata.get("hierarchies", [])
        self.data = self.JSONdata.get("data", [])
        self.measures = self.JSONdata.get("measures", [])
        self.metainfo = self.JSONdata.get("metainfo", {})
        self.id_consulta = self.metainfo.get("id")
        
        # Registro de mensajes de log en aplicaciones
        self.logger = logging.getLogger(f'{self.__class__.__name__} [{self.id_consulta}]')

    def get_elements_of_response(self):
        """
        Devuelve los elementos principales de la respuesta en un diccionario.
        """
        self.logger.info("Presentación de los datos en diccionario.")
        return {
            "jerarquias": self.hierarchies,
            "medidas": self.measures,
            "metainfo": self.metainfo,
            "id_consulta": self.id_consulta,
            "datos": self.data
        }
    
    def process_measures_columns(self, df_data):
        """
        Procesa columnas que contienen diccionarios, extrayendo el valor asociado a la clave 'val', 
        y reorganiza dichas columnas para que aparezcan al final del DataFrame.
        Además, convierte los valores de las columnas correspondientes a 'measures' a texto, 
        reemplazando los puntos por comas.
    
        Parámetros:
            df (pd.DataFrame): El DataFrame que será modificado.
            measures (list of dicts): Lista de nombres de columnas que contienen diccionarios con la clave 'val'.
                             Estas columnas serán procesadas y reordenadas al final del DataFrame.
            self.measures_columns : guarda las columnas de medidas para que el último método muestre el dataset completo ordenado. 
    
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
        n_measures = len(self.measures)
        measures_des = []
        for i in range(n_measures):
            col = self.measures[i]["des"]
            measures_des.append(col)
            if col in df_data.columns:
                # Extraer el valor de la clave 'val' en cada fila
                df_data[col] = df_data[col].apply(lambda x: x.get('val') if isinstance(x, dict) else x)
                # Convertir valores a texto y reemplazar puntos por comas
                df_data[col] = df_data[col].astype(str).str.replace('.', ',', regex=False)
        
        self.measure_columns = list(map(functions.clean_text, measures_des))
        # Reorganizar las columnas: mover las columnas de `measures` al final
        remaining_columns = [col for col in df_data.columns if col not in measures_des]
        df_data = df_data[remaining_columns + measures_des]
        
        return df_data
    
    def get_DataFrame_dataJSON(self, process_measures = False):
        """
        Transforma los datos JSON estructurados según las jerarquías en un DataFrame de Pandas,
        añadiendo columnas de códigos (_cod) para las jerarquías y procesando las columnas de medidas
        para extraer los valores de la clave 'val' y reorganizarlas al final del DataFrame.
        
        Este método realiza los siguientes pasos:
        1. Construye un DataFrame a partir de los datos estructurados según las jerarquías y las medidas.
        2. Agrega columnas para los códigos de las jerarquías, basándose en los valores 'cod' presentes en los datos.
        3. Llama a la función `process_measures_columns` para procesar las columnas de medidas y reorganizarlas.
        
        Parámetros:
            Ninguno.
        
        Retorna:
            pd.DataFrame: El DataFrame procesado con las columnas de medidas reorganizadas y las columnas de códigos añadidas.
        
        Funcionalidad:
            - Se crean las columnas de códigos asociadas a cada jerarquía. Estas columnas contienen los valores de 'cod'
              extraídos de los datos si están presentes.
            - Se aseguran de que las columnas de medidas estén al final del DataFrame, y los valores de estas columnas
              se actualizan con los valores asociados a la clave 'val' de los diccionarios presentes en cada celda.
        
        Ejemplo de uso:
            >>> data = {
            ...     "Nombre": ["A", "B", "C"],
            ...     "Edad": [25, 30, 22],
            ...     "Medida1": [{"val": 100, "format": "100"}, {"val": 200, "format": "200"}, {"val": 300, "format": "300"}],
            ...     "Medida2": [{"val": 10, "format": "10"}, {"val": 20, "format": "20"}, {"val": 30, "format": "30"}]
            ... }
            >>> obj = YourClassName()
            >>> obj.data = data  # Asumimos que este es el formato de los datos
            >>> obj.hierarchies = [{"alias": "Nombre"}, {"alias": "Edad"}]  # Ejemplo de jerarquías
            >>> obj.measures = [{"des": "Medida1"}, {"des": "Medida2"}]  # Ejemplo de medidas
            >>> df = obj.get_DataFrame_dataJSON()
            >>> print(df)
              Nombre  Edad  Medida1  Medida2  Nombre_cod  Edad_cod
            0      A    25      100       10        None      None
            1      B    30      200       20        None      None
            2      C    22      300       30        None      None
        
        Notas:
            - El DataFrame resultante tiene columnas adicionales para los códigos de las jerarquías, que contienen los
              valores extraídos de las claves 'cod' en los datos JSON.
            - Las columnas de medidas, especificadas por el atributo `measures`, se procesan y reordenan al final del DataFrame.
            - Si alguna columna de medidas no contiene un diccionario con la clave 'val', se mantendrá su valor original sin cambios.
            - Este método hace uso de la función `process_measures_columns` para reorganizar las columnas de medidas.
        """
        self.logger.info('Transformando los datos JSON a DataFrame')
        # Obtener nombres de las columnas
        columnas_jerarquia = [jerarquia["alias"] for jerarquia in self.hierarchies]
        columnas_medida = [medida["des"] for medida in self.measures]
        columnas = columnas_jerarquia + columnas_medida

        # Crear el DataFrame de los datos
        try:
            df = pd.DataFrame(self.data, columns=columnas)
        except Exception as e:
            print(f'Consulta sin datos - {self.id_consulta}')
            raise e

        # Agregar columnas de códigos
        col_to_cod = [col for col in columnas if col not in columnas_medida]
        col_cod = [col + "_cod" for col in col_to_cod]

        for c, c_c in zip(col_to_cod, col_cod):
            df[c_c] = df[c].apply(lambda x: x["cod"] if isinstance(x, dict) and "cod" in x else None)
        
        if process_measures:
            df = self.process_measures_columns(df)
        
        self.logger.info('Datos Transformados a DataFrame Correctamente')
        self.df_data = df
        return df
    
    def clean_cod_combination(self, cod_combination):
        """
        Función para limpiar la combinación de códigos, eliminando "Total" y "TOTAL".
        De la tabla final de jerarquías. 
        """
        return [item for item in cod_combination if item not in ["Total", "TOTAL"]]

    @staticmethod
    def request_hierarchies_values(hierarchy_element):
        """
        Realiza una solicitud a la URL de una jerarquía específica para obtener sus valores.
        """
        url = hierarchy_element.get("url")
        response = requests.get(url)
        return response.json()

    def process_hierarchy_level(self, alias, node, cod_combination=None, descriptions=None, level=1):
        """
        Función recursiva para procesar los niveles de jerarquía y construir una estructura de datos organizada.
        
        Este método procesa recursivamente los niveles de jerarquía de un nodo, actualizando las combinaciones
        de códigos y las descripciones, y almacenando los resultados en una lista de diccionarios. Cada nivel de
        jerarquía se agrega a la lista de resultados, con información como el identificador del nodo, la combinación
        de códigos correspondiente, y las descripciones en las columnas Des1, Des2, etc.
    
        Parámetros:
            alias (str): El alias que identifica la jerarquía, utilizado como valor en la columna "Variable".
            node (dict): El nodo de la jerarquía actual que contiene información sobre el nodo y sus hijos.
                        Debe tener las claves "cod", "label", "id", "isLastLevel" y "children".
            cod_combination (list, opcional): La combinación de códigos acumulada desde niveles anteriores. Se utiliza
                                               para mantener la relación de códigos a lo largo de los niveles. Por defecto, es una lista vacía.
            descriptions (list, opcional): Las descripciones acumuladas desde niveles anteriores. Se utiliza para mantener
                                           la relación de descripciones a lo largo de los niveles. Por defecto, es una lista vacía.
            level (int, opcional): El nivel actual en la jerarquía. Se usa para generar las columnas Des1, Des2, etc., 
                                   y para controlar la profundidad de la recursión. El valor por defecto es 1.
    
        Retorna:
            Ninguno: Este método no retorna un valor, sino que agrega diccionarios con los resultados a `self.result_rows`.
        
        Funcionalidad:
            1. Si no se proporcionan `cod_combination` o `descriptions`, se inicializan como listas vacías.
            2. Se actualiza la combinación de códigos y las descripciones actuales para el nivel procesado.
            3. Se crea una fila con los valores de "Variable", "id", "COD_combination", y las descripciones correspondientes 
               (Des1, Des2, etc.).
            4. La fila creada se agrega a la lista de resultados `self.result_rows`.
            5. Si el nodo actual no es el último nivel (`isLastLevel` es False) y tiene hijos, se llama recursivamente 
               a la función para procesar los niveles inferiores de la jerarquía.
    
        Ejemplo de uso:
            >>> node = {
            >>>     "cod": "001",
            >>>     "label": "Nivel1",
            >>>     "id": "id001",
            >>>     "isLastLevel": False,
            >>>     "children": [
            >>>         {"cod": "001.1", "label": "Subnivel1", "id": "id001.1", "isLastLevel": True, "children": []},
            >>>         {"cod": "001.2", "label": "Subnivel2", "id": "id001.2", "isLastLevel": True, "children": []}
            >>>     ]
            >>> }
            >>> handler.process_hierarchy_level("Pais", node)
            >>> print(handler.result_rows)
            [{"Variable": "Pais", "id": "id001", "COD_combination": ["001"], "Des1": "Nivel1"},
             {"Variable": "Pais", "id": "id001.1", "COD_combination": ["001", "001.1"], "Des1": "Nivel1", "Des2": "Subnivel1"},
             {"Variable": "Pais", "id": "id001.2", "COD_combination": ["001", "001.2"], "Des1": "Nivel1", "Des2": "Subnivel2"}]
    
        Notas:
            - El método se llama recursivamente para procesar todos los niveles de la jerarquía.
            - Las combinaciones de códigos y descripciones se acumulan a medida que se desciende por los niveles de jerarquía.
            - Este método actualiza la lista `self.result_rows`, que debe estar previamente definida en la clase.
        """
        if cod_combination is None:
            cod_combination = []
        if descriptions is None:
            descriptions = []

        # Actualiza la combinación de códigos y la descripción para este nivel
        current_cod_combination = cod_combination + [node["cod"]]
        current_descriptions = descriptions + [node["label"]]

        # Prepara la fila para agregar al resultado
        row = {
            "Variable": alias,
            "id": node["id"],
            "COD_combination": current_cod_combination,
        }

        # Completa las columnas Des1, Des2, etc., con las descripciones actuales
        for i in range(1, level + 1):
            row[f"Des{i}"] = current_descriptions[i - 1] if i <= len(current_descriptions) else None

        # Añade la fila al DataFrame de resultados
        self.result_rows.append(row)

        # Verifica que haya más niveles antes de llamar recursivamente
        if not node["isLastLevel"] and node["children"]:
            for child in node["children"]:
                self.process_hierarchy_level(alias, child, current_cod_combination, current_descriptions, level + 1)

    def process_all_hierarchies(self):
        """
        Función principal para procesar todas las jerarquías y devolver el DataFrame limpio.
    
        Este método recorre todas las jerarquías definidas en `self.hierarchies`, procesa cada una de ellas
        utilizando la función `process_hierarchy_level` para desglosar los diferentes niveles de jerarquía, 
        y almacena los resultados en un DataFrame estructurado. Además, se limpia la columna 'COD_combination' 
        para eliminar valores como 'Total' o 'TOTAL', y guarda el DataFrame final como un atributo de la clase.
    
        Parámetros:
            Ninguno. El método utiliza los atributos internos de la clase (`self.hierarchies` y `self.result_rows`).
    
        Retorna:
            pd.DataFrame: Un DataFrame que contiene los resultados procesados de todas las jerarquías,
                          con las combinaciones de códigos y descripciones organizadas.
    
        Funcionalidad:
            1. Recorre todas las jerarquías definidas en `self.hierarchies`.
            2. Para cada jerarquía, obtiene los datos relacionados a través de `request_hierarchies_values`.
            3. Verifica si los datos obtenidos son un diccionario, y en ese caso, llama a `process_hierarchy_level`
               para procesar los niveles de jerarquía de forma recursiva.
            4. Los resultados de todos los niveles de jerarquía se almacenan en `self.result_rows`.
            5. Al final, los datos procesados se convierten en un DataFrame y se limpia la columna 'COD_combination'
               eliminando valores no deseados como 'Total' y 'TOTAL'.
            6. El DataFrame final se guarda como el atributo `self.hierarchies_info_df`.
    
        Ejemplo de uso:
            >>> handler.process_all_hierarchies()
            >>> print(handler.hierarchies_info_df)
            # El DataFrame resultante contendrá las combinaciones de códigos y las descripciones
            # procesadas de todas las jerarquías definidas en `self.hierarchies`.
    
        Notas:
            - Este método depende de la función `request_hierarchies_values` para obtener los datos
              correspondientes a cada jerarquía. Los datos deben estar en un formato adecuado.
            - La columna 'COD_combination' es limpiada para asegurar que no contenga valores como 'Total' o 'TOTAL'.
            - El método modifica el atributo `self.hierarchies_info_df` con el DataFrame final.
            - La estructura del DataFrame resultante incluye las combinaciones de códigos y descripciones
              correspondientes a cada jerarquía procesada.
        """
        hierarchies = self.hierarchies
        self.result_rows = []

        # Procesar cada jerarquía
        for hier in hierarchies:
            alias = hier["alias"]
            example_data = self.request_hierarchies_values(hier)
            parent_data = example_data["data"]

            # Confirma que parent_data es un diccionario y lo pasa directamente
            if isinstance(parent_data, dict):
                self.process_hierarchy_level(alias, parent_data)
            else:
                print(f"parent_data no es un diccionario: {parent_data}")

        # Convertir la lista de resultados en un DataFrame final
        df = pd.DataFrame(self.result_rows)

        # Limpiar la columna 'COD_combination' para eliminar 'Total' y 'TOTAL'
        df["COD_combination"] = df["COD_combination"].apply(self.clean_cod_combination)
        
        # Guardarlo como atributo
        self.hierarchies_info_df = df

        # Devolver el DataFrame limpio
        return df

    def save_hierarchies_level(self, path, level = None):
        """
        Método para guardar la tabla de desagregación y aplanamiento de jerarquías.
        Si `level` está vacío, guarda la tabla completa. Si contiene un valor, 
        guarda solo las filas correspondientes al nivel especificado.
        Si la tabla `hierarchies_info_df` no existe o está vacía, llama al método 
        `process_all_hierarchies` para generarla.

        Parámetros:
            path (str): Ruta donde se guardará el archivo Excel.
            level (str, opcional): Nivel de jerarquía a filtrar. Si es None, se guarda la tabla completa.
        """
        # Verificar si la tabla de jerarquías existe y está llena
        if not hasattr(self, 'hierarchies_info_df') or self.hierarchies_info_df.empty:
            print("La tabla de jerarquías no está disponible o está vacía. Procesando jerarquías...")
            self.process_all_hierarchies()

        # Trabajar con la tabla procesada
        df_hier = self.hierarchies_info_df

        # Filtrar por nivel si se proporciona
        if level:
            df_hier = df_hier[df_hier['Variable'] == level]

        # Guardar el resultado en Excel
        df_hier.to_excel(path, index=False)
    
    def union_by_cod_combination(self, df1_data, df2_hier, col1, col2):
        """
        Une dos DataFrames utilizando dos columnas que contienen códigos jerárquicos.
        
        Esta función se utilizará en el MAPEO DE LAS JERARQUIAS Y LOS DATOS. 
    
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
        result = functions.remove_column(result, col1)
        # Borramos las columnas comunes cuando se unen todas las jerarquías "COD_COMBINATION"
        result = functions.remove_column(result, "COD_combination")
        return result
    
    def map_data_w_hierarchies_info(self):
        """
        Mapea los datos del conjunto de datos original (`self.dataset`) con la información de las jerarquías 
        (`self.hierarchies_info_df`) para integrar los valores jerárquicos dentro del DataFrame de medidas 
        y códigos, y organiza las columnas resultantes.
    
        Este método realiza la normalización de los nombres de las columnas, la selección de las columnas relevantes
        para el análisis, la preparación de la información de las jerarquías y la unión de los datos según los códigos
        de jerarquía. Al final, genera un DataFrame mapeado que incluye tanto los datos originales como los valores jerárquicos.
    
        Parámetros:
            Ninguno. El método utiliza los atributos de la clase (`self.dataset`, `self.measures`, `self.hierarchies_info_df`).
    
        Retorna:
            pd.DataFrame: Un DataFrame con los datos originales mapeados con la información de las jerarquías, 
                          con las columnas organizadas para su análisis.
    
        Funcionalidad:
            1. Normaliza los nombres de las columnas en `self.dataset` utilizando la función `functions.norm_columns_name`.
            2. Prepara el nombre de las columnas de las medidas de acuerdo con las descripciones de `self.measures`.
            3. Filtra las columnas relevantes para el análisis, incluyendo las columnas de códigos (`_cod`) y las medidas.
            4. Prepara la información de las jerarquías para cada nivel, extrayendo los nombres de las columnas de descripciones
               y filtrando la información correspondiente de `self.hierarchies_info_df`.
            5. Mapea las columnas de códigos de jerarquía con los valores jerárquicos en `self.hierarchies_info_df`, 
               renombrando las columnas de descripción y uniendo los DataFrames de acuerdo con los códigos de combinación.
            6. Al final, devuelve el DataFrame mapeado que incluye tanto los códigos como las descripciones jerárquicas.
    
        Ejemplo de uso:
            >>> handler.map_data_w_hierarchies_info()
            >>> print(handler.df_data_mapped)
            # El DataFrame resultante contendrá las columnas de medidas y códigos, con las descripciones jerárquicas mapeadas.
    
        Notas:
            - El proceso depende de las columnas de códigos (que terminan en '_cod') y las medidas que se definen en `self.measures`.
            - La información de las jerarquías se extrae de `self.hierarchies_info_df` y se mapea a las columnas de códigos.
            - El DataFrame resultante contiene tanto las columnas originales como las nuevas columnas con las descripciones jerárquicas.
            - El método genera y guarda el DataFrame mapeado como el atributo `self.df_data_mapped`.
        """
        # Preparar la tabla de datos para el mapeo.
        self.df_data = functions.norm_columns_name(self.df_data)
        n_medidas = len(self.measures)
            # Generar nombre normalizado para las columnas de medidas 
        if n_medidas == 1: 
            col_medida = [functions.clean_text(self.measures[0]["des"])]
        else:
            col_medida = [functions.clean_text(self.measures[i]["des"]) for i in range(n_medidas)]
        
        # Disminuir la información del dataset para ahorrar coste computacional, 
        # filtrando por las columnas que nos interesan para el análisis. 
        cod_columns = self.df_data.filter(regex='_cod$').columns.tolist()
        selected_columns = cod_columns + col_medida
        cod_df = self.df_data[selected_columns].copy()
        
        # Preparación de la información de las jerarquías
# =============================================================================
#         if self.hierarchies_info_df.empty(): 
#             self.process_all_hierarchies()
# =============================================================================
            
        hierarchies_used = pd.unique(self.hierarchies_info_df.Variable)
        name_cols = [re.search(r'D(?:_AA)?_(.*?)_0', elem).group(1) for elem in hierarchies_used]
        
        # esas columnas de codificación de jerarquía serán mapeadas según la jerarquía con la tabla hierarchies_info_df.
        for alias, col in zip(hierarchies_used, name_cols):
            # Filtramos toda la información de las jerarquías según la que nos encontremos mapeando
            hier_df_values = self.hierarchies_info_df[self.hierarchies_info_df["Variable"] == alias].copy()
                # En el proceso de mapeo no necesitamos las columnas "Variable" ni "id" porque no aportan información al conjunto de datos. 
            columns_to_keep = hier_df_values.columns.difference(["Variable", "id"])
            hier_df_values = hier_df_values[columns_to_keep]
                # Renombramos las columnas de valores categóricos para que queden organizadas según la jerarquía. 
            cols_to_rename = hier_df_values.filter(regex = r'^Des\d+$').columns
            rename_dict = {col_name : col_name.replace('Des', col) for col_name in cols_to_rename}
            hier_df_values = hier_df_values.rename(columns = rename_dict)
            
            # Unión del conjunto de datos por "COD_combination" en la tabla de valores de jerarquías y por la columna "_cod" correspondiente a la jerarquía
            cod_df = self.union_by_cod_combination(cod_df, hier_df_values, alias + "_cod", "COD_combination")
            
        remaining_col = [col for col in cod_df.columns if col not in self.measure_columns]
        cod_df = cod_df[remaining_col + self.measure_columns]
        self.df_data_mapped = cod_df.copy()
        return cod_df
        
        



