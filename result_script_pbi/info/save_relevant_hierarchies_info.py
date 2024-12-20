import requests
import pandas as pd
import logging
import re
import os 

os.chdir("C:/Users/AnaBorrego/Desktop/Proyectos/ANDALUCIA_EMPRENDE/VISOR/BADEA_2D/")
from src import main

file = "C:/Users/AnaBorrego/Desktop/Proyectos/ANDALUCIA_EMPRENDE/VISOR/id_año.xlsx" # ruta + nombre : donde queremos guardar el fichero en cuestión. 
hierarchie_to_save = "D_TEMPORAL_0" # valor de nombre de la jerarquía de interés. 

url = "https://www.juntadeandalucia.es/institutodeestadisticaycartografia/intranet/admin/rest/v1.0/consulta/44804?"

# params = {
#     "D_TEMPORAL_0" : "180156,180175,180194",
#     "D_AA_TERRITROIO_0" : "515892,515902",
#     "D_SEXO_0" : "3691,3689,3690",
#     "posord" : "f[D_AA_TERRITROIO_0],f[D_TEMPORAL_0],f[D_SEXO_0],f[D_EDAD_0],f[D_AA_DURAULTEMPR_0],c[Measures]"
# }

response = requests.get(url)

handler = main.APIDataHandler(response)
dataset_aux = handler.get_DataFrame_dataJSON(process_measures = True) 
processed_data = handler.process_all_hierarchies()

# Método implementado ------------
handler.save_hierarchies_level(path = file, level = hierarchie_to_save)

# Lógica manual --------------------------
# Guardar valores de una jerarquía en concreto
# hierar_i = handler.hierarchies_info_df
# hierar_i[hierar_i['Variable'] == hierarchie_to_save].to_excel(file, index = False)

# # Guardar fichero aplanado y mapeado final. 
# handler.df_data_mapped.to_excel(file, index = False)

