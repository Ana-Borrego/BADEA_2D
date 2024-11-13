# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 11:20:04 2024

@author: Ana Borrego
"""

import requests
import pandas as pd
import logging

class APIDataHandler:
    def __init__(self, response):
        """
        Constructor que toma la respuesta JSON de una consulta API y extrae sus elementos clave.
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

    def get_DataFrame_dataJSON(self):
        """
        Devuelve un DataFrame con los datos estructurados según las jerarquías,
        incluyendo columnas "_cod" para los códigos de los valores de las jerarquías.
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

        self.logger.info('Datos Transformados a DataFrame Correctamente')
        self.df_data = df
        return df

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
        Función recursiva para procesar los niveles de jerarquía.
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

    def clean_cod_combination(self, cod_combination):
        """
        Función para limpiar la combinación de códigos, eliminando "Total" y "TOTAL".
        """
        return [item for item in cod_combination if item not in ["Total", "TOTAL"]]


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
# 1. Crea una instancia de la clase APIHandler pasando la respuesta `response` de la API
# =============================================================================

handler = APIDataHandler(response)

# =============================================================================
# 2. Procesa las jerarquías llamando al método `process_all_hierarchies`
# =============================================================================

processed_data = handler.process_all_hierarchies()

# =============================================================================
# 4. Guardar el dataframe resultante con `.to_excel()`
# =============================================================================
# cleaned_data.to_excel("jerarquias_limpiadas.xlsx")
# =============================================================================






