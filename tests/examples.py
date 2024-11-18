# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 12:21:55 2024

@author: Ana Borrego
"""
import requests
import pandas as pd
import re
import os
os.chdir("C:/Users/Ana Borrego/Desktop/proyectos/andalucia_emprende/VISOR/request_py/")
import src.main  as ahd

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
# =============================================================================
# handler.response
# handler.JSONdata
# handler.hierarchies
# handler.data
# handler.measures
# handler.metainfo
# handler.id_consulta
# =============================================================================

# =============================================================================
# 2. Consulta de los elementos en formato diccionario. 
# =============================================================================

dict_response = handler.get_elements_of_response()
# =============================================================================
# dict_response
# dict_response["jerarquias"]
# dict_response["id_consulta"]
# =============================================================================

# =============================================================================
# 3. Consulta de datos que devuelve BADEA tras la petición, en formato DataFrame:
# =============================================================================

dataset = handler.get_DataFrame_dataJSON(process_measures = True) 
# =============================================================================
# handler.df_data
# =============================================================================

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

final_dataset = handler.map_data_w_hierarchies_info()
