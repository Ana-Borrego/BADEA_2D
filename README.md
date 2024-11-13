# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 11:28:44 2024

@author: Ana Borrego
"""

# Directorio de trabajo. 

+ `auxiliar_script/`: 
    + `aplanar_jerarquias.py` : fichero donde se realizó la generalización del aplanamiento de las jerarquías en dimensión 2D. 
    + `connection_IECA_auxiliar.py` : conjunto de acciones realizadas para entender la definición de la clase.
    + `datos_script_apoyo.py` : script de IECA_extractor que se ha utilizado como apoyo para la definición de la clase final y el tratamiento de la respuesta proporcionada por BADEA.
    + `def_class_for_response.py` : clase APIDataHandler en una versión beta, sin aplanamiento de las jerarquías, sólo con métodos de manejo de información.
    + `functions_BADEA.py` : recoge las funciones de la clase final en un único script.
+ `class_APIDataHandler_complete.py` : modelo final de definición de clase con todos los métodos implementados. 

------------------------------------------------------------------------

# Class APIDataHandler. 

La clase `APIDataHandler` maneja el procesamiento y transformación de datos obtenidos tras una consulta a la API del Instituo Estadístico y Cartográfico de Andalucía (BADEA). Permite extraer, procesar, limpiar y organizar los datos en un `DataFrame` de Pandas para su posterior análisis. 

## 1. Métodos definidos en la clase. 

1. `__init__(self, response)` : 
    + **Descripción**: Este es el consultor de la clase. Toma como entrada una respuesta de la consulta a la API de BADEA, de forma que guarda su respuesta en JSON, y extrae los elementos clave de la misma para almacenarlos en atributos de la clase. 
    + **Parámetros**: 
        + `response` : obejto de respuesta de `requests.get` a la url de consulta de la API. 
    + **Atributos**: 
        + `self.response`: respuesta original de la API. Parámetro de entrada.
        + `self.JSONdata`: Copia de los datos JSON de la respuesta de la API. 
        + `self.hierarchies`: Información referente a las jerarquías extraídas de respuesta de la consulta. 
        + `self.data`: Datos prinicpales extraídos de la respuesta de la consulta.
        + `self.measures`: Medidas asociadas a los datos. 
        + `self.metainfo`: Información adicional sobre la consulta, ofrecida directamente en la respuesta de la misma. 
        + `self.id_consulta`: ID de la tabla consultada en la API.
        + `self.logger`: Configuración del registro de logs. 

2. `.get_elements_of_response(self)`: 
    + **Descripción**: Devuelve un diccionario con los elementos clave de la respuesta de la API. Facilita el conocimiento de la estructura de respuesta a consultas de la API. 
    + **Parámetros**:
        + No precisa de parámetros de entrada.
    + **Atributos**: 
        + No crea atributos de clase, se trata de facilitar la visualización de la respuesta. 
    + **Retorno**: 
        + Un diccionario con las claves `"jerarquias"`, `"medidas"`, `"metainfo"`, `"id_consulta"` y `"datos"`, correspondientes a los elementos de la respuesta. 

3. `.get_DataFrame_dataJSON(self)`: 
    + **Descripción**: Convierte los datos JSON en un `DataFrame` de Pandas, estructurado de acuerdo a las jerarquías (distintas desagregaciones) y a las medidas definidas en la respuesta. 
    + **Parámetros**: 
        + No precisa de parámetros de entrada. 
    + **Atributos** : 
        + `self.data_df`: data.frame estructurado de los datos de la respuesta según las columnas de jerarquías y las de medidas.
    + **Proceso** : 
        + Extrae los alias de las jerarquías y las descripciones de las medidas para usarlas como columnas del `DataFrame`. 
        + Añade columnas adiccionales que continenen los códigos (`cod`) de los valores de las jerarquías, si estos estuvieran disponibles. *En principio estos códigos deberían estar siempre disponibles.*
    + **Retorno**: 
        + Un `DataFrame` de Pandas con las columnas correspondientes a las jerarquías y las medidas, además de las columnas de códigos.

4. `.request_hierarchies_values(hierarchy_element)`:
    + **Descripción**: Realiza una solicitud HTTP a la URL de una jerarquía específica para obtener sus valores y su información. Está pensada para usarla dentro de otras funciones para aplanar las jerarquías, pero está como **método estático** para poder usarlo de manera individual sin necesidad de construir la clase, de esta forma si se precisa la consulta de alguna jerarquía en concreto es accesible. 
    + **Parámetros**: 
        + `hierarchy_element`: Diccionario que contiene a la URL de la jerarquía. Esto debe seguir la estructura de un elemento de `self.jerarquias`. 
    + **Atributos**:
        + No crea atributos de clase.
    + **Retorno**:
        + La respuesta JSON de la consulta al url de la jerarquía en cuestión. 

5. `process_hierarchy_level(self, alias, node, cod_combination = None, descriptions = None, level = 1)`:
    + **Descripción**: Método recursivo para procesas los niveles de jerarquía de una jerarquía específica, construyendo la combinación de códigos y descripciones para cada nivel de forma que facilite el mapeo en el conjunto de datos final del que sacamos los valores "_cod" (`self.data_df`)
    + **Parámetros**:
        + `alias`: Alias de la jerarquía. Esto facilitará saber a qué columna de `self.data_df` está referidos los niveles de jerarquía, facilitando así el mapeo. 
        + `node`: Nodo actual de la jerarquía que se está procesando. 
        + `cod_combination`: Lista acumulativa de códgios (se va actualizando a medida que se procesan los niveles)
        + `descriptions`: Lista acumulativa de descripciones (valores que tomará la variable) (se va actualizando)
        + `level`: Nivel actual de la jerarquía que se está procesando. 
    + **Atributos**: 
        + `self.result_rows` : lista con diccionarios sobre las informaciones resultantes de cada paso del proceso recursivo. Esto erá útil para utilizar la función en todas las jerarquías a la vez.
    + **Proceso**:
        + La función recursivamente procesa cada nodo de la jerarquía, actualizando la combinación de códigos y descripciones hasta que se alcanza el último nivel de desagregación. 

6. `process_all_hierarchies(self)`:
    + **Descripción**: Función principal que procesa todas las jerarquías y devuelve un `DataFrame` limpio, con toda la información relevante para 
    + **Parámetros**:
    + **Atributos**:
    + **Proceso**:
    + **Retorno**:
