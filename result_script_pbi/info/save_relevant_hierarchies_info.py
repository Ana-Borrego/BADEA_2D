import requests
import pandas as pd
import logging
import re
import os 

os.chdir("C:/Users/Ana Borrego/Desktop/proyectos/andalucia_emprende/VISOR/request_py/")
from src import main

file = "path/to/your/file.xlsx" # ruta + nombre : donde queremos guardar el fichero en cuestión. 
hierarchie_to_save = "hierarchie_cod" # valor de nombre de la jerarquía de interés. 

url = "https://www.juntadeandalucia.es/institutodeestadisticaycartografia/intranet/admin/rest/v1.0/consulta/44804?"

params = {
    "D_TEMPORAL_0" : "180156,180175,180194",
    "D_AA_TERRITROIO_0" : "515892,515902",
    "D_SEXO_0" : "3691,3689,3690",
    "posord" : "f[D_AA_TERRITROIO_0],f[D_TEMPORAL_0],f[D_SEXO_0],f[D_EDAD_0],f[D_AA_DURAULTEMPR_0],c[Measures]"
}

response = requests.get(url, params = params)

handler = main.APIDataHandler(response)
dataset_aux = handler.get_DataFrame_dataJSON(process_measures = True) 
processed_data = handler.process_all_hierarchies()

# Guardar valores de una jerarquía en concreto
hierar_i = handler.hierarchies_info_df
hierar_i[hierar_i['Variable'] == hierarchie_to_save].to_excel(file, index = False)

# Guardar fichero aplanado y mapeado final. 
handler.df_data_mapped.to_excel(file, index = False)