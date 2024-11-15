# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 08:23:49 2024

@author: Ana Borrego
"""
import pandas as pd
import requests

# Función para obtener los datos de las jerarquías: Done
def request_hierarchies_values(hierarchy_element):
    url = hierarchy_element.get("url")
    response = requests.get(url)
    return response.json()

# Función recursiva para procesar los niveles de jerarquía
def process_hierarchy_level(alias, node, cod_combination=None, descriptions=None, level=1):
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
    result_rows.append(row)

    # Verifica que haya más niveles antes de llamar recursivamente
    if not node["isLastLevel"] and node["children"]:
        for child in node["children"]:
            process_hierarchy_level(alias, child, current_cod_combination, current_descriptions, level + 1)

# Función principal para procesar todas las jerarquías
def process_all_hierarchies(response):
    JSONdata = response.json()
    hierarchies = JSONdata["hierarchies"]
    global result_rows
    result_rows = []

    for hier in hierarchies:
        alias = hier["alias"]
        example_data = request_hierarchies_values(hier)
        parent_data = example_data["data"]

        # Confirma que parent_data es un diccionario y lo pasa directamente
        if isinstance(parent_data, dict):
            process_hierarchy_level(alias, parent_data)
        else:
            print(f"parent_data no es un diccionario: {parent_data}")
            
    # # aquí le podríamos aplicar directamente clean_cod_combination() definida posteriormente
    
    # Convertir la lista de resultados en un DataFrame final
    return pd.DataFrame(result_rows) 


# Ejemplo de uso
# response = requests.get("URL_CON_DATOS_JSON")  # Obtener datos de la API
# processed_data = process_all_hierarchies(response)
# processed_data


# Ejemplo de uso
url = "https://www.juntadeandalucia.es/institutodeestadisticaycartografia/intranet/admin/rest/v1.0/consulta/44804?D_TEMPORAL_0=180194&D_AA_TERRITROIO_0=515892&D_SEXO_0=3691,3689,3690&posord=f[D_AA_TERRITROIO_0],f[D_TEMPORAL_0],f[D_SEXO_0],f[D_EDAD_0],f[D_AA_DURAULTEMPR_0],c[Measures]"

# parámetros de consulta: 
params = {
    "D_TEMPORAL_0" : "180194",
    "AA_TERRITROIO_0" : "515892",
    "D_SEXO_0" : "3691,3689,3690",
    "posord" : "f[D_AA_TERRITROIO_0],f[D_TEMPORAL_0],f[D_SEXO_0],f[D_EDAD_0],f[D_AA_DURAULTEMPR_0],c[Measures]"
}

response = requests.get(url, params = params)  # Obtener datos de la API
processed_data = process_all_hierarchies(response)

# Función para limpiar la combinación de códigos, eliminando "Total" y "TOTAL"
def clean_cod_combination(cod_combination):
    # Filtrar los elementos que no sean "Total" o "TOTAL"
    return [item for item in cod_combination if item not in ["Total", "TOTAL"]]

# Aplicar la función a la columna 'COD_combination'
processed_data["COD_combination"] = processed_data["COD_combination"].apply(clean_cod_combination)

# Mostrar el resultado
print(processed_data["COD_combination"])

processed_data.to_excel("jerarquias.xlsx")

