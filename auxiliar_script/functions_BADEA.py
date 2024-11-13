# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 11:02:54 2024

@author: Ana Borrego
"""

import os
import sys
import requests

import pandas as pd

import logging
import numpy as np

def get_elements_of_response(response):
    JSONdata = response.json().copy()
    hierarchies = JSONdata["hierarchies"]
    data = JSONdata["data"]
    measures = JSONdata["measures"] # valor final a consultar según las desagregaciones # "des"
    metainfo = JSONdata["metainfo"]
    id_consulta = JSONdata["metainfo"]["id"]
    return {
            "jerarquias" : hierarchies,
            "medidas" : measures,
            "metainfo" : metainfo,
            "id_consulta" : id_consulta,
            "datos" : data
        }

def get_DataFrame_dataJSON(response):
    """
    Función que tomando de entrada la respuesta de la consulta REQUEST.GET a la url
    devuelve el data.frame con los datos estructurados según las jerarquías, además de 
    columnas "_cod" que recogen los códigos de los valores de las jerarquías para su posterior 
    mapeo con las desagreagciones.
    """
    JSON = response.json().copy()
    jerarquias = JSON["hierarchies"]
    medidas = JSON["measures"]
    data = JSON["data"]
    id_consulta = JSON["metainfo"]["id_consulta"]
    
    # Get columns names
    columnas_jerarquia = [jerarquia["alias"] for jerarquia in jerarquias]
    columnas_medida = [medida["des"] for medida in medidas]
    columnas = columnas_jerarquia + columnas_medida
    
    # Create dataframe of the data values using the columns defined
    try:
        df = pd.DataFrame(data, columns=columnas)
    except Exception as e:
        print('Consulta sin datos - %s', id_consulta)
        raise e
    
    # Get codes for the breakdown level
    col_to_cod = [col for col in columnas if col not in columnas_medida]  
    col_cod = [col + "_cod" for col in col_to_cod]

    for c, c_c in zip(col_to_cod, col_cod): 
        df[c_c] = df[c].apply(lambda x: x["cod"])  
        
    return df

# Función para obtener los datos de las jerarquías: Done
def request_hierarchies_values(hierarchy_element):
    url = hierarchy_element.get("url")
    response = requests.get(url)
    return response.json()

# =============================================================================
# def request_hierarchies_values(hierarchie_element):
#     """
#     Pasando el diccionario de la jerarquía de la que se desee hacer la consulta, realiza la consulta para obtener los valores. 
#     """
#     url_consulta = hierarchie_element["url"]
#     # hacer la consulta 
#     jerarq_response = requests.get(url_consulta)
#     jq_info = jerarq_response.json()
#     return jq_info
#  
# =============================================================================


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
