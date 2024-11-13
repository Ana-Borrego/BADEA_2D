# -*- coding: utf-8 -*-
"""
Created on Fri Nov  8 14:04:21 2024

@author: Ana Borrego
"""

import os
import sys
import requests

import pandas as pd

import logging
import numpy as np

# =============================================================================
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)
# =============================================================================

def get_values_jq_w_get(hierarchies_element):
     url_consulta = hierarchies_element["url"]
     # hacer la consulta 
     jerarq_response = requests.get(url_consulta)
     jq_info = jerarq_response.json()
     return jq_info
 
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
    
    # Get columns names
    columnas_jerarquia = [jerarquia["alias"] for jerarquia in jerarquias]
    columnas_medida = [medida["des"] for medida in medidas]
    columnas = columnas_jerarquia + columnas_medida
    
    # Create dataframe of the data values using the columns defined
    try:
        df = pd.DataFrame(data, columns=columnas)
    except Exception as e:
        print('Consulta sin datos')
        raise e
    
    # Get codes for the breakdown level
    col_to_cod = [col for col in columnas if col not in columnas_medida]  
    col_cod = [col + "_cod" for col in col_to_cod]

    for c, c_c in zip(col_to_cod, col_cod): 
        df[c_c] = df[c].apply(lambda x: x["cod"])  
        
    return df


# ---------------------------------------
# url JSON de consulta
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


all_hierarchies = JSONdata["hierarchies"]
# Tarda tela ---
values_hierarchies = {
        "alias" : [data["alias"] for data in all_hierarchies], 
        "info" : [get_values_jq_w_get(value) for value in all_hierarchies]
    }
# ----

all_info = pd.DataFrame(values_hierarchies)

# Trabajamos con una jerarquía:
ejemplo1 = get_values_jq_w_get(all_hierarchies[0])
alias_ejemplo1 = values_hierarchies["alias"][0]

# Definición de qué queremos conseguir: 
result = pd.DataFrame(columns = ["Variable", "id","COD_combination", "Des1", "Des2", "Des3"])

padre = pd.DataFrame(ejemplo1["data"])
# =============================================================================
# padre.columns
# Out[16]: 
# Index(['id', 'cod', 'label', 'des', 'order', 'parentId', 'level', 'levelId',
#        'validFrom', 'validTo', 'isLastLevel', 'children'],
#       dtype='object')
# =============================================================================

# id : sirve para la consulta, rellenar el url en ese parámetro con el filtro deseado
#      sirve para relacionar con el id del padre (si hubiera) 
# cod : sirve para cruzar después con los datos
# label : valor
# des : valor presentado en la tabla
# order : orden en la jerarquía
# parentId : si es nulo se trata del valor padre, si no es nulo se trata del ID del padre, sirve para relacionar hijos-padres.
# level : nivel de desagregación. 
# validFrom : 
# validTo : 
# isLastLevel : booleano que indica si existe mayor desagregación (si tiene hijos).
# children : diccionario que contiene a los hijos. 

# cada padre es una combinación de cod. 
# cada hijo tendrá una combinación de cod igual a [codigodelpadre, codigodelhijo]
# si fuera hijo de hijo de un padre: [codigodelpadre, codigodelhijopadre, codigodelhijoencuestión] y así. 

row_father = pd.DataFrame({
        "Variable": alias_ejemplo1,
        "id" : padre.id.astype(str),
        "COD_combination" : padre.cod.astype(str).to_list(),
        "Des1" : padre.label.to_list(),
        "Des2" : None,
        "Des3" : None
    })

# Los valores None es porque la combinación de código hace referencia al valor padre, sin desagregación en hijos. 
# En este caso el valor padre es un valor que realmente no queremos

result = pd.concat([result, row_father], ignore_index = True)

# Ahora pasamos a los distintos hijos que tiene esta desagregación. 
# Normalmente, se tiene directamente un único hijo que contiene las distintas desagregaciones, pero se comprueba: 
if len(padre.children.tolist()) == 1: 
    print("Contiene un único hijo")
else:
    print("Precisa de estudio.")

des1 = pd.DataFrame(padre.children[0])

# Cúantos hijos tiene el padre ? 
# =============================================================================
# des1.shape[0]
# Out[43]: 2
# =============================================================================
# En este caso el proceso se haría para dos hijos. 
des1.cod = des1.cod.astype(str)

columns_to_export = ["id", "cod", "label"]
check = des1[columns_to_export].nunique() == 1
if check.all():
    row_child1 = pd.DataFrame({
            "Variable": alias_ejemplo1,
            "id" : des1.id.astype(str).to_list()[0],
            "COD_combination" : des1.cod.to_list()[0],
            "Des1" : padre.label,
            "Des2" : des1.label.to_list()[0],
            "Des3" : None
        })
else:
    row_child1 = pd.DataFrame({
            "Variable": [alias_ejemplo1]*2,
            "id" : des1.id.astype(str).to_list(),
            "COD_combination" : des1.cod.to_list(),
            "Des1" : [padre.label[0]]*2,
            "Des2" : des1.label.to_list(),
            "Des3" : [None, None]
        })

result = pd.concat([result, row_child1], ignore_index = True)

# Ahora la cosa se complica porque cada hijo tiene hijos: 
if not des1.iloc[0]["isLastLevel"] and des1.iloc[0,]["children"] != []:
    print("El primer hijo tiene hijos")
if des1.iloc[1]["isLastLevel"] == False and des1.iloc[1,]["children"] != []:
    print("El segundo hijo tiene hijos")
# =============================================================================
# if not des1.iloc[0]["isLastLevel"] and des1.iloc[0,]["children"] != []:
#     print("El primer hijo tiene hijos")
# if des1.iloc[1]["isLastLevel"] == False and des1.iloc[1,]["children"] != []:
#     print("El segundo hijo tiene hijos")
# 
# 
# El primer hijo tiene hijos
# El segundo hijo tiene hijos
# =============================================================================

# Hay que sacar los valores de los hijos (de la misma manera) pero para cada valor:

des1_1 = pd.DataFrame(des1.iloc[0].children)
des1_1.cod = des1_1.cod.astype(str)
des1_2 = pd.DataFrame(des1.iloc[1].children)
des1_2.cod = des1_2.cod.astype(str)

row_child1_1_2 = pd.DataFrame({
        "Variable": [alias_ejemplo1]*2,
        "id" : [des1_1.id.astype(str)[0], des1_2.id.astype(str)[0]],
        "COD_combination" : [[des1.cod[0] , des1_1.cod[0]], [des1.cod[0] , des1_2.cod[0]]],
        "Des1" : [padre.label[0]]*2, # Padre total
        "Des2" : des1.label.to_list(),
        "Des3" : [des1_1.label[0], des1_2.label[0]]
    })

result = pd.concat([result, row_child1_1_2])

# Función para procesar los padres y crear la primera fila del DataFrame
def process_parent(alias, parent_data):
    row_father = pd.DataFrame({
        "Variable": alias,
        "id": parent_data.id.astype(str),
        "COD_combination": parent_data.cod.astype(str).to_list(),
        "Des1": parent_data.label.to_list(),
        "Des2": None,
        "Des3": None
    })
    return row_father

# Función para procesar hijos de primer nivel y añadirlos al DataFrame
def process_first_level_children(alias, parent_data, level=1):
    children = pd.DataFrame(parent_data.children[0])
    columns_to_export = ["id", "cod", "label"]
    des_column = f"Des{level + 1}"

    # Comprobar si todos los valores en las columnas de los hijos son únicos
    if children[columns_to_export].nunique().all() == 1:
        row_child = pd.DataFrame({
            "Variable": alias,
            "id": children.id.astype(str).iloc[0],
            "COD_combination": [children.cod.astype(str).iloc[0]],
            "Des1": parent_data.label,
            des_column: children.label.iloc[0],
            "Des3": None
        })
    else:
        row_child = pd.DataFrame({
            "Variable": [alias] * len(children),
            "id": children.id.astype(str),
            "COD_combination": children.cod.astype(str),
            "Des1": [parent_data.label[0]] * len(children),
            des_column: children.label.to_list(),
            "Des3": [None] * len(children)
        })
    
    return row_child

# Función para procesar hijos de segundo nivel y añadirlos al DataFrame
def process_second_level_children(alias, parent_data, parent_data_global, sub_children, level):
    if level == 2:
        row_child = pd.DataFrame({
            "Variable": [alias] * len(sub_children),
            "id": sub_children.id.astype(str),
            "COD_combination": [[parent_data.cod[i], sub_children.cod[i]] for i in range(len(sub_children))],
            "Des1": [parent_data.label[0]] * len(sub_children),
            "Des2": parent_data.label,
            "Des3": sub_children.label
        })
    else: 
        row_child = pd.DataFrame({
            "Variable": [alias],
            "id": sub_children.loc["id"].astype(str),
            "COD_combination": '[' + parent_data.cod + "," + sub_children.loc["cod"] + "]",
            "Des1": parent_data_global.label[0],
            "Des2": parent_data.label,
            "Des3": sub_children.loc["label"]
        })
    return row_child


# Función principal para procesar todas las jerarquías
def process_all_hierarchies(response):
    JSONdata = response.json()
    hierarchies = JSONdata["hierarchies"]

    result = pd.DataFrame(columns=["Variable", "id", "COD_combination", "Des1", "Des2", "Des3"])

    for hier in hierarchies:
        alias = hier["alias"]
        example_data = get_values_jq_w_get(hier)
        parent_data = pd.DataFrame(example_data["data"])

        # Procesa el padre y los hijos de primer nivel
        row_father = process_parent(alias, parent_data)
        result = pd.concat([result, row_father], ignore_index=True)
        
        parent_data_global = parent_data.copy()
        
        row_children = process_first_level_children(alias, parent_data)
        result = pd.concat([result, row_children], ignore_index=True)

        # sacamos los hijos de este primer nivel: 
        child1 = pd.DataFrame(parent_data.children[0])
        child2 = pd.DataFrame(child1.children.to_list())
        
        n_childs = child2.shape[0]
        for i in range(n_childs):
            c1 = child2.iloc[i]
            row_c1 = process_second_level_children(alias = alias, parent_data = child1.iloc[0], parent_data_global = parent_data_global, sub_children = c1, level = 3)
            result = pd.concat([result, row_c1], ignore_index=True)
            lastlevel = c1.loc["isLastLevel"]
            children_value = c1.loc["children"]
            while lastlevel == False and children_value != []:
                other_children = pd.DataFrame(children_value)
                n_children = other_children.shape[0] 
                for j in range(n_children):
                    c2 = other_children.iloc[j,] # Serie
                    row_c2 = process_second_level_children()
            
        
    return result

























# =============================================================================
# for i in range(2): #len(values_hierarchies["alias"])
#    
#     alias = values_hierarchies["alias"][i]
#     info = values_hierarchies["info"][i]
#     df_info = pd.DataFrame(info)
#     df_toextend = df_info.loc[:,["id", 'cod', 'label', 'des', 'order']]
#     df_toextend["id"] = df_toextend["id"].astype(str)
#    
#     if alias == 'D_TEMPORAL_0': 
#         df_info = df_info[df_info["des"].astype(int) >= 2010]
#         df_info["alias"] = alias
#         hierarchies_df.append(df_info)
#     elif alias == 'D_AA_TERRITROIO_0':
#         parent_df = pd.DataFrame(df_info["children"][0])
#         to_extend = parent_df.loc[:, ["id", "cod", "des", 'label', 'parentId']]
#         to_extend["id"] = to_extend["id"].astype(str)
#         to_extend.columns = "parent_" + to_extend.columns
#         to_extend = pd.merge(df_toextend, to_extend, right_on = "parent_parentId", left_on = "id", how = "right")
#         children = None
#         for j in range(parent_df.shape[0]): 
#             if parent_df["isLastLevel"][j] == False:
#                 children1 = pd.DataFrame(parent_df.iloc[j, ]["children"])
#                 children1_toextend = children1.loc[:, ['id', 'cod', 'label', 'des', 'order', 'parentId']]
#                 children1_toextend["parentId"] = children1_toextend["parentId"].astype(str)
#                 children1_toextend.columns = "child1_" + children1_toextend.columns
#                 df_aux = pd.merge(children1_toextend, to_extend, left_on = "child1_parentId", right_on = "parent_id")
# 
#                 if not children1["isLastLevel"].all(): 
#                     children2 = pd.DataFrame(children1["children"][0])
#                     children2_toextend = children2.loc[:, ['id', 'cod', 'label', 'des', 'order', 'parentId']]
#                     children2_toextend.columns = "child2_" + children2_toextend.columns
#                     if children2["isLastLevel"].all(): 
#                         children2_toextend["child2_parentId"] = children2_toextend["child2_parentId"].astype(str)
#                         df_aux.child1_id = df_aux.child1_id.astype(str)
#                         children_aux = pd.merge(df_aux, children2_toextend, left_on = "child1_id", right_on="child2_parentId", how = "left")
#                         if children is None: 
#                             children = children_aux.copy()
#                         else:
#                             children = pd.concat([children, children_aux])
#         children["alias"] = alias
#         hierarchies_df.append(children)
#     
# =============================================================================
 
# columna booleana que nos dice si el código de jerarquía es padre o hijo del valor padre dado por los 2 primeros elementos del código. 
# =============================================================================
# for col in columnas_jerarquia: 
#     df[col + "_bool"] = [1 if len(row["cod"]) > 2 else 0 for row in df[col]]
# df
# =============================================================================
     
  
                            
# =============================================================================
# common_cols = ['D_AA_TERRITROIO_0', 'D_TEMPORAL_0', 'D_SEXO_0', 'D_EDAD_0',]
# desagregation_col = list(set(columnas_jerarquia).symmetric_difference(set(common_cols)))
# 
# cods = [row[0] for row in df[desagregation_col].values])
# 
# =============================================================================

# =============================================================================
# data[4]
# jerarquias[0]
# values = None
# 
# jq_episodio = jerarquias[4]
# jq_territorio = jerarquias[0]
# 
# def get_values_jq_w_get(jq):
#     url_consulta = jq["url"]
#     # hacer la consulta: 
#     jerarq_response = requests.get(url_consulta)
#     jq_info = jerarq_response.json()
#     relevant_info = jq_info["data"]["children"][0]["children"]
#     return relevant_info
# 
# values_territorio = get_values_jq_w_get(jq_territorio)
# values_episodio = get_values_jq_w_get(jq_episodio)
# 
# for jq in jerarquias: 
#     # Obtener el "alias" de la jerarquía en cuestión que se va a consultar: 
#     alias = jq["alias"]
#     descripcion = jq["des"]
#     # Obtener la url de consulta de valores de la jerarquía
#     url_consulta = jq["url"]
#     # hacer la consulta: 
#     jerarq_response = requests.get(url_consulta)
#     jq_info = jerarq_response.json()
#     relevant_info = jq_info["data"]["children"][0]["children"]
#     
#     if alias == 'D_AA_DURAULTEMPR_0':
#         for c in range(len(relevant_info)):
#             parent_name = relevant_info[c]["des"]
#             parent_id = relevant_info[c]["id"]
#             
#             # lista de hijos de la jerarquía
#             list_children = relevant_info[c]["children"]
#             # sub valores 
#             num_subvalues = len(list_children)
#             
#             # get subvalues
#             for j in range(num_subvalues):
#                 
#                 
#             
#     for i in range(len(relevant_info)):
#         if values is None:
#             # Crear el DataFrame inicial en la primera iteración
#             values = pd.DataFrame(
#                 [{
#                     "id": relevant_info[i]["id"],
#                     "alias" : alias,
#                     "descripcion" : descripcion,
#                     "cod": relevant_info[i]["cod"],
#                     "label": relevant_info[i]["label"],
#                     "des": relevant_info[i]["des"]                
#                 }]
#             )
#         else:
#             # Crear un DataFrame de una sola fila para agregar al dataframe con los valores de las jerarquias.
#             new_row = pd.DataFrame([{
#                 "id": relevant_info[i]["id"],
#                 "alias" : alias,
#                 "descripcion" : descripcion,
#                 "cod": relevant_info[i]["cod"],
#                 "label": relevant_info[i]["label"],
#                 "des": relevant_info[i]["des"]
#             }])
#             
#             # Concatenar el nuevo DataFrame de fila única con el DataFrame existente
#             values = pd.concat([values, new_row], ignore_index=True)
#         
#     
# values
# 
# 
# =============================================================================

    
    
