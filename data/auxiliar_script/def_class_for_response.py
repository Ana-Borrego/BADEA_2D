# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 11:40:39 2024

@author: Ana Borrego
"""

# =============================================================================
# diccionario_periodicidad = {'Mensual': 'M', 'Anual': 'A',
#                                  'Mensual  Fuente: Instituto Nacional de Estadística': 'M', '': 'M',
#                                  'Anual. Datos a 31 de diciembre': 'A'}
# =============================================================================

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

        return df
        self.logger.info('Datos Transformados a DataFrame Correctamente')

    """
    1. ¿Cuándo usar un @staticmethod?
        Cuando la función realiza una tarea relacionada con la clase, pero no depende de los atributos de la clase o de la instancia.
        Si el comportamiento del método es completamente independiente de los datos del objeto 
        (como en este caso, donde simplemente realiza una consulta GET usando los datos pasados).
    2. Llamar al método estático
        Puedes llamar a un método estático de dos maneras:
            1. Desde la clase directamente (sin instanciarla):
                result = APIDataHandler.request_hierarchies_values(hierarchie_element)
            2. Desde una instancia de la clase (aunque no es necesario):
                handler = APIDataHandler(response)
                result = handler.request_hierarchies_values(hierarchie_element)
    En este caso, @staticmethodhace que el método sea accesible sin necesidad de crear un objeto de la clase, 
    ya que no depende del estado del objeto.
    """
    @staticmethod
    def request_hierarchies_values(hierarchy_element):
        """
        Realiza una solicitud a la URL de una jerarquía específica para obtener sus valores.
        """
        url = hierarchy_element.get("url")
        response = requests.get(url)
        return response.json()
    


# =============================================================================
# # Ejemplo de manejo: 
# handler = APIDataHandler(response)
# # static method ---------------------------------------------
# con la clase instanciada:
#   result = handler.request_hierarchies_values(hierarchie_element)
# ó 
#  sin instanciar la clase:
#   result = APIDataHandler.request_hierarchies_values(hierarchie_element) 
# =============================================================================

url = "https://www.juntadeandalucia.es/institutodeestadisticaycartografia/intranet/admin/rest/v1.0/consulta/44804?D_TEMPORAL_0=180194&D_AA_TERRITROIO_0=515892&D_SEXO_0=3691,3689,3690&posord=f[D_AA_TERRITROIO_0],f[D_TEMPORAL_0],f[D_SEXO_0],f[D_EDAD_0],f[D_AA_DURAULTEMPR_0],c[Measures]"

# parámetros de consulta: 
params = {
    "D_TEMPORAL_0" : "180194",
    "AA_TERRITROIO_0" : "515892",
    "D_SEXO_0" : "3691,3689,3690",
    "posord" : "f[D_AA_TERRITROIO_0],f[D_TEMPORAL_0],f[D_SEXO_0],f[D_EDAD_0],f[D_AA_DURAULTEMPR_0],c[Measures]"
}

## ¿Es posible definir una función que saque los params?

# Realizar request GET
response = requests.get(url, params = params)

## control de verificación de consulta
if response.status_code == 200:
    JSONdata = response.json().copy()
else: 
    print(f"Error en la solicitud: {response.status_code}")


handler = APIDataHandler(response)
handler.get_elements_of_response()
data = handler.get_DataFrame_dataJSON()

