# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 09:56:04 2024

@author: Ana Borrego
"""
import requests
import pandas as pd
import logging
import re

url ='https://www.juntadeandalucia.es/institutodeestadisticaycartografia/intranet/admin/rest/v1.0/consulta/13514?'

params = {
    "D_TEMPORAL_0" : "180156,180175,180194,180213",
    "posord" : "f[D_SEXO_0],f[D_CNED2014_0],f[D_EPA_NIVEL_0],f[D_TEMPORAL_0],f[D_EDAD_0],c[Measures],p[D_EPA_RELACTIVIDAD_0],p[D_TERRITORIO_0]"
}

# Realizar request GET
response = requests.get(url, params = params)

# Auxiliar functions for processing data in APIHandlerData
def clean_text(texto):
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
    # Renombrar columnas aplicando la función de limpieza
    df.columns = [clean_text(col) for col in df.columns]
    return df

def remove_column(df, column_name):
    if column_name in df.columns:
        return df.drop(columns=[column_name])
    else:
        print(f"La columna '{column_name}' no existe en el DataFrame.")
        return df
    
class APIDataHandler:
    
    def __init__(self, response):
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

    def process_measures_columns(self, df_data):
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
        
        self.measure_columns = list(map(clean_text, measures_des))
        # Reorganizar las columnas: mover las columnas de `measures` al final
        remaining_columns = [col for col in df_data.columns if col not in measures_des]
        df_data = df_data[remaining_columns + measures_des]
        
        return df_data
    
    def get_DataFrame_dataJSON(self, process_measures = False):
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
        return [item for item in cod_combination if item not in ["Total", "TOTAL", "P1_00"]]

    @staticmethod
    def request_hierarchies_values(hierarchy_element):
        url = hierarchy_element.get("url")
        response = requests.get(url)
        return response.json()

    def process_hierarchy_level(self, alias, node, cod_combination=None, descriptions=None, level=1):
        if cod_combination is None:
            cod_combination = []
        if descriptions is None:
            descriptions = []

        # Actualiza la combinación de códigos y la descripción para este nivel
        current_cod_combination = cod_combination + [node["cod"]]
        current_descriptions = descriptions + [node["des"]]

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

    def save_hierarchies_level(self, path):
        self.hierarchies_info_df.to_excel(path)
    
    def union_by_cod_combination(self, df1_data, df2_hier, col1, col2):
        # Procesamos las columnas que van a hacer la unión para que python las entienda y pueda encontrar sus coincidencias.
        for df, col in zip([df1_data, df2_hier], [col1, col2]):
            df[col] = df[col].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
        result = pd.merge(df1_data, df2_hier, left_on = col1, right_on= col2, how = 'left')
        
        # Eliminar las columnas que no tienen valores porque la jerarquía 
        # no utiliza todas las desagregaciones en el dato de consulta
        result = result.dropna(axis = 1, how = "all")
        # df de datos sin la columna de codificación correspondiente
        result = remove_column(result, col1)
        # Borramos las columnas comunes cuando se unen todas las jerarquías "COD_COMBINATION"
        result = remove_column(result, "COD_combination")
        return result
    
    def map_data_w_hierarchies_info(self):
        # Preparar la tabla de datos para el mapeo.
        self.df_data = norm_columns_name(self.df_data)
        n_medidas = len(self.measures)
            # Generar nombre normalizado para las columnas de medidas 
        if n_medidas == 1: 
            col_medida = [clean_text(self.measures[0]["des"])]
        else:
            col_medida = [clean_text(self.measures[i]["des"]) for i in range(n_medidas)]
        
        # Disminuir la información del dataset para ahorrar coste computacional, 
        # filtrando por las columnas que nos interesan para el análisis. 
        cod_columns = self.df_data.filter(regex='_cod$').columns.tolist()
        selected_columns = cod_columns + col_medida
        cod_df = self.df_data[selected_columns].copy()
        
        # Preparación de la información de las jerarquías            
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


# Requests - Final result
handler = APIDataHandler(response)
dataset_aux = handler.get_DataFrame_dataJSON(process_measures = True) 
processed_data = handler.process_all_hierarchies()
dataset = handler.map_data_w_hierarchies_info()

del dataset_aux, processed_data

