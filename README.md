Este proyecto está desarrollado para facilitar la implementación de consultas a la API del Instituto Estadístico y Cartográfico de Andalucía, denominada BADEA, en PowerBI, de forma que se facilite el acceso para la visualización de indicadores, a la vez que obtener un visualizador de actualización automática o semi-automática. 

Consiste en un análisis, siguiendo una estructura de proceso ETL, y una limpieza de las respuestas a las consultas de la API mediante un aplanamiento de los datos recibidos tras la consulta. 

# Directorio de trabajo. 

+ `data/`: directorio que contiene toda la información relevante a la creación del proyecto. 
    + `auxiliar_script/`: 
        + `aplanar_jerarquias.py` : fichero donde se realizó la generalización del aplanamiento de las jerarquías en dimensión 2D. 
        + `connection_IECA_auxiliar.py` : conjunto de acciones realizadas para entender la definición de la clase.
        + `datos_script_apoyo.py` : script de IECA_extractor que se ha utilizado como apoyo para la definición de la clase final y el tratamiento de la respuesta proporcionada por BADEA.
        + `def_class_for_response.py` : clase APIDataHandler en una versión beta, sin aplanamiento de las jerarquías, sólo con métodos de manejo de información.
        + `functions_BADEA.py` : recoge las funciones de la clase final en un único script.
        + `codigo_mapeo_jerarquia.py` : código utilizado para generalizar el método de mapeo de los datos y las jerarquías en cuestión. 
+ `imgs/`: directorio que contiene las imágenes para la creación del `README.md`
+ `src/`: raíz del proyecto para su implementación. 
    + `main.py` : modelo final de tratamiento de consultas. Listo para la implementación en proyecto local. 
    + `functions.py`: funciones necesarias para la implementación de ciertos métodos del tratamiento de datos de `main.py`
+ `tests/`: pruebas realizadas para la verificación de la funcionalidad.  
    + `examples.py` : casos de uso.

---

# 0. Instrucciones para la instalación. 

1. **Clona el repositorio:** 

```
git clone https://github.com/Ana-Borrego/BADEA_2D.git
```

2. **Crea un entorno virtual** (opcional pero recomendado)

    Si deseas evitar conflictos con las dependencias de otros proyectos, es recomendable crear un entorno virtual. Usa los siguientes comandos según tu sistema operativo:

Para sistemas basados en Unix (Linux/macOS):
```bash
python3 -m venv venv
source venv/bin/activate
```
Para Windows:

```bash
python -m venv venv
.\venv\Scripts\activate
```

3. **Instala las dependencias**: Una vez dentro del entorno virtual (o en tu entorno global de Python si no usas uno), instala las dependencias listadas en el archivo requirements.txt ejecutando el siguiente comando:

```bash
pip install -r requirements.txt
```

**¡Listo!**
Ahora ya tienes todas las dependencias necesarias instaladas para trabajar con el proyecto.

*** 

## 1. `src/main.py` class APIHandlerData. 

*En cada método dentro del fichero se podrá consultar indicaciones sobre los procedimientos y ejemplos sencillos de acciones.*

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

3. `.process_measures_columns(self, df_data)`:
    + **Descripción**: Procesa columnas en un DataFrame que contienen diccionarios, extrayendo los valores asociados a la clave 'val' de cada elemento. Además, reorganiza las columnas especificadas en self.measures para que se ubiquen al final del DataFrame.
    + **Parámetros**:
        + `df_data (pd.DataFrame)`: El DataFrame de entrada que será modificado. Contiene las columnas de jerarquías y medidas especificadas.
    + **Atributos**:
        + `self.measures`: Lista de nombres de columnas que contienen valores en formato de diccionario (normalmente con claves como 'val' y 'format').
    + **Proceso**:
        + Recorre cada columna definida en `self.measures`.
        + Si una columna contiene valores en formato de diccionario (`dict`):
            + Extrae el valor asociado a la clave `'val'`.
            + Si el valor no es un diccionario, se mantiene el valor original.
        + Reorganiza las columnas del `DataFrame` moviendo las especificadas en self.measures al final.
    + **Retorno** : Devuelve el `DataFrame` procesado.

4. `.get_DataFrame_dataJSON(self, process_measures = bool)`: 
    + **Descripción**: Convierte los datos JSON en un `DataFrame` de Pandas, estructurado de acuerdo a las jerarquías (distintas desagregaciones) y a las medidas definidas en la respuesta. 
    + **Parámetros**: 
        + `process_measures`: (por defecto `False`) booleano para especificar si utilizar el procesador de las columnas de medidas y obtener únicamente el valor. 
    + **Atributos** : 
        + `self.data_df`: data.frame estructurado de los datos de la respuesta según las columnas de jerarquías y las de medidas.
    + **Proceso** : 
        + Extrae los alias de las jerarquías y las descripciones de las medidas para usarlas como columnas del `DataFrame`. 
        + Añade columnas adiccionales que continenen los códigos (`cod`) de los valores de las jerarquías, si estos estuvieran disponibles. *En principio estos códigos deberían estar siempre disponibles.*
            + Según `process_measures` la/s columna/s referida/s a medida/s son procesadas o no. 
    + **Retorno**: 
        + Un `DataFrame` de Pandas con las columnas correspondientes a las jerarquías y las medidas, además de las columnas de códigos.

5. `.clean_cod_combination(self, cod_combination)`:
    + **Descripción**: Función que realmente no tiene interés en su acceso, se utiliza exclusivamente dentro del método `.process_all_hierarchies` para la limpieza de las combinaciones de códigos. 
    + **Parámetros**:
        + `cod_combination`: la combinación de código que se pretende limpiar. (lista)
    + **Atributos**:
        + No crea atributos de clase.
    + **Proceso**:
        + Elimina los valores de `cod_combination` que sean texto. 
    + **Retorno**:
        + La lista `cod_combination` sin los valores textos.

6. `.request_hierarchies_values(hierarchy_element)`: ```@staticmethod```
    + **Descripción**: Realiza una solicitud HTTP a la URL de una jerarquía específica para obtener sus valores y su información. Está pensada para usarla dentro de otras funciones para aplanar las jerarquías, pero está como **método estático** para poder usarlo de manera individual sin necesidad de construir la clase, de esta forma si se precisa la consulta de alguna jerarquía en concreto es accesible. 
    + **Parámetros**: 
        + `hierarchy_element`: Diccionario que contiene a la URL de la jerarquía. Esto debe seguir la estructura de un elemento de `self.jerarquias`. 
    + **Atributos**:
        + No crea atributos de clase.
    + **Retorno**:
        + La respuesta JSON de la consulta al url de la jerarquía en cuestión. 

7. `.process_hierarchy_level(self, alias, node, cod_combination = None, descriptions = None, level = 1)`:
    + **Descripción**: Método recursivo para procesas los niveles de jerarquía de una jerarquía específica, construyendo la combinación de códigos y descripciones para cada nivel de forma que facilite el mapeo en el conjunto de datos final del que sacamos los valores "_cod" (`self.data_df`)
    + **Parámetros**:
        + `alias`: Alias de la jerarquía. Esto facilitará saber a qué columna de `self.data_df` está referidos los niveles de jerarquía, facilitando así el mapeo. 
        + `node`: Nodo actual de la jerarquía que se está procesando. 
        + `cod_combination`: Lista acumulativa de códgios (se va actualizando a medida que se procesan los niveles)
        + `descriptions`: Lista acumulativa de descripciones (valores que tomará la variable) (se va actualizando)
        + `level`: Nivel actual de la jerarquía que se está procesando. 
    + **Atributos**: 
        + `self.result_rows` : accede a este atributo creado en el método `.process_all_hierarchies()`.
    + **Proceso**:
        + La función recursivamente procesa cada nodo de la jerarquía, actualizando la combinación de códigos y descripciones hasta que se alcanza el último nivel de desagregación.

8. `.process_all_hierarchies(self)`:
    + **Descripción**: Función principal que procesa todas las jerarquías y devuelve un `DataFrame` limpio, con toda la información relevante para el mapeo en `self.data_df` y conseguir el `DataFrame` final con los valores de categorías según las jerarquías en el método final. 
    + **Parámetros**:
        + No precisa de parámetros de entrada. 
    + **Atributos**:
        + `self.result_rows`: lista con diccionarios sobre las informaciones resultantes de cada paso del proceso recursivo. Esto erá útil para utilizar la función en todas las jerarquías a la vez. Estos elementos formarán el `DataFrame` final con la información referida a todas las jerarquías. Lo crea y rellena de forma recursiva haciendo uso del método recursivo de la clase `.process_hierarchy_level()`.
        + `self.hierarchies_info_df`: Resultado final sobre la información de las jerarquías utilizadas en los datos de respuestas de consultas.
    + **Proceso**:
        + Se recorren todas las jerarquías en la respuesta y se procesan de una en una. 
        + Para cada jerarquía, se obtienen los valores y se procesan gracias al método `.process_hierarchy_level()`.
        + Una vez procesas todas las jerarquías, se contruye un `DataFrame` a partir de las filas generadas. 
        + Se limpia la columna `COD_combination`, eliminando las combinaciones que en lugar de código de categoría contienen "Total" o "TOTAL" gracias al método `.clean_cod_combination` y su aplicación por filas. *Esto viene de que las jerarquías padres simplemente es información sobre la jerarquía y en lugar de tener un valor "cod" real, tienen un texto que hace referencia a TOTAL, y esto no es útil para el mapeo. Es más sencillo limpiar la columna que especificarlo en el análisis.*
        + Se genera el `DataFrame` final con la información de la jerarquía y se guarda/asigna como atributo de clase, para su acceso en posteriores métodos. 
    + **Retorno**:
        + Un `DataFrame` de Pandas con una estructura sencilla para el mapeo de códigos en el conjunto final de datos.
            + Columna `alias`: contiene el alias de la jerarquía a la que hace referencia los distintos niveles (mostrados por filas).
            + Columna `id`: id del nivel en cuestión tratado en la fila.
            + Columna `COD_combination`: lista con los códigos referentes al nivel del valor de la jerarquía, de forma que ofrece información de quién es el padre y del camino recorrido para llegar a dicho valor de nivel. 
            + Columnas `Des{i}`: Son los distintos valores categóricos de los grupos recorridos para llegar al último nivel. Está relacionado directamente con la combinación de códigos `COD_combination`. Toma el valor None cuando no hay un nivel de desagregación mayor a `i`. 

9. `.save_hierarchies_level(self, path)`: 
    + Método para exportar la información de categorías de jerarquías.

10. `.map_data_w_hierarchies_info(self)`: 
    + **Descripción**: Mapea los datos originales (`self.dataset`) con la información de las jerarquías almacenada en `self.hierarchies_info_df`. Este método integra los valores jerárquicos dentro del conjunto de datos, normaliza los nombres de las columnas, y organiza las columnas para facilitar el análisis.
    + **Parámetros**:
        + No recibe parámetros externos. Utiliza los **atributos** de la clase:
            + `self.dataset`: Contiene los datos originales a mapear.
            + `self.measures`: Lista de medidas relevantes para el análisis.
            + `self.hierarchies_info_df`: Información adicional sobre las jerarquías que será utilizada en el mapeo.
    + **Atributos**: 
        + `self.df_data_mapped`: Almacena el `DataFrame` resultante con los datos mapeados y organizados.
    + **Proceso**: 
        + Normaliza los nombres de las columnas en `self.dataset` utilizando la función `functions.norm_columns_name`.
        + Prepara el nombre de las columnas de las medidas de acuerdo con las descripciones de `self.measures`.
        + Filtra las columnas relevantes para el análisis, incluyendo las columnas de códigos (`_cod`) y las medidas.
        + Prepara la información de las jerarquías para cada nivel, extrayendo los nombres de las columnas de descripciones y filtrando la información correspondiente de `self.hierarchies_info_df`.
        + Mapea las columnas de códigos de jerarquía con los valores jerárquicos en `self.hierarchies_info_df`, renombrando las columnas de descripción y uniendo los `DataFrame`'s de acuerdo con los códigos de combinación.
        + Al final, devuelve el `DataFrame` mapeado que incluye tanto los códigos como las descripciones jerárquicas.
    + **Retorno**: 
        + Un `DataFrame` con los datos originales mapeados con la información de las jerarquías, con las columnas organizadas para su análisis.

***

## 2. Flujo de trabajo para obtener los datos aplanados y mapeados. 

### 2.1. Inicialización.

Llamar al constructor `__init__` con la respuesta de la consulta `requests.get()` a la API. 
+ Guarda la respuesta de la API en un objeto JSON y extrae los elementos clave: jerarquías, datos, medidas, y metainformación.

**Resultado**: Los datos crudos y las jerarquías están disponibles como atributos de la clase (`self.data`, `self.hierarchies`, etc.). 

### 2.2. Visualización de los elementos clave (opcional). 

Se pueden obtener los elementos clave de la respuesta usando el método `.get_elements_of_response` y visualizarlos de una forma más clara. 

**Resultado**: Diccionario con claves como "jerarquias", "medidas", "metainfo", y "datos".

### 2.3. Procesamiento inicial de los datos brutos de la respuesta de la API.  

Llamar a `.get_DataFrame_dataJSON()` para estructurar los datos JSON en un DataFrame.
+ El método `.get_DataFrame_dataJSON` toma los datos de la respuesta y los convierte en un `DataFrame` de Pandas. Este `DataFrame` incluye tanto los datos estructurados como los códigos de las jerarquías, gracias a un proceso de extracción interno de los mismos. 
+ *Las columnas de código tendrán como sufijo `_cod`*. 
+ Si `process_measures=True`, se procesan las columnas de medidas (extrae el valor asociado a la clave 'val' en las columnas relevantes).

**Resultado**: Un `DataFrame` estructurado con columnas correspondientes a las jerarquías y medidas

### 2.4. Procesar jerarquías. 

Llamar a `.process_all_hierarchies()` para extraer y aplanar la información jerárquica.
+ Este método utiliza el método recursivo `.process_hierarchy_level` para recorrer los niveles jerárquicos y extraer las combinaciones de códigos y descripciones.
+ El método `.process_all_hierarchies` procesa todas las jerarquías, creando combinaciones de códigos y descripciones para el mapeo o la visualización de los valores posibles de los parámetros de consulta (correspondientes a la integración del valor `id` (siguiendo formato "{id_1},{id_2},{id_3}") en el parámetro de `params` deseado). También limpia la columna `'COD_combination'` eliminando valores no deseados (gracias a `.clean_cod_combination()`) para facilitar el mapeo. 


**Resultado**: consiste en un `DataFrame` con la información jerárquica mapeada

*Nota:* El resultado puede ser descargado en el formato deseado, y/o consultado mediante el atributo `self.hierarchies_info_df`, para la generación de nuevas consultas. Además el resultado es utilizado en el método final de aplanamiento en 2 dimensiones de los datos de consulta devueltos por BADEA. 

### 2.5. Mapeo de datos con jerarquías. 

Llamar a `.map_data_w_hierarchies_info()` para integrar los datos originales con la información de las jerarquías.
+ Este método utiliza los códigos jerárquicos en `self.data_df` y mapea las descripciones jerárquicas desde `self.hierarchies_info_df`.

**Resultado**: El `DataFrame` final (`self.df_data_mapped`) está listo para análisis o exportación.

### Resultados intermedios y final. 

+ `self.data_df`: Datos iniciales estructurados con jerarquías y medidas.
+ `self.hierarchies_info_df`: Información jerárquica aplanada, lista para mapeo.
+ `self.df_data_mapped`: Dataset final aplanado, con jerarquías y datos combinados.

#### Ejemplo de flujo completo. 

``` python
from src/main.py import APIHandlerData  # Asegúrate de importar la clase desde el módulo correcto

# Paso 1: Inicializar la clase con la respuesta de la API
handler = APIHandlerData(response, params = {})

# Paso 2: (Opcional) Explorar los elementos clave de la respuesta
elementos = handler.get_elements_of_response()
print(elementos)  # Visualizar elementos clave

# Paso 3: Procesar y estructurar los datos en un DataFrame
handler.get_DataFrame_dataJSON(process_measures=True)
print(handler.data_df)  # Ver los datos iniciales procesados

# Paso 4: Procesar las jerarquías y aplanarlas
handler.process_all_hierarchies()
print(handler.hierarchies_info_df)  # Ver las jerarquías procesadas
handler.save_hierarchies_level("url/de/exportacion")

# Paso 5: Mapear los datos originales con las jerarquías procesadas
handler.map_data_w_hierarchies_info()
print(handler.df_data_mapped)  # Dataset final aplanado

# Resultado final
dataset_final = handler.df_data_mapped
```

## 3. Casos de Uso. Ejemplos. 

Se puede ver el código en `examples.py`

```
url = "https://www.juntadeandalucia.es/institutodeestadisticaycartografia/intranet/admin/rest/v1.0/consulta/44804?"
# parámetros de consulta: 
params = {
    "D_TEMPORAL_0" : "180194",
    "AA_TERRITROIO_0" : "515892",
    "D_SEXO_0" : "3691,3689,3690",
    "posord" : "f[D_AA_TERRITROIO_0],f[D_TEMPORAL_0],f[D_SEXO_0],f[D_EDAD_0],f[D_AA_DURAULTEMPR_0],c[Measures]"
}

# Realizar request GET
response = requests.get(url, params = params)
```

1. Inicialización:

```
handler = APIDataHandler(response)
# <__main__.APIDataHandler at 0x18d4ff3fd70>
```

![estructura de la clase](./imgs/handler.jpg)

Acceso a los atributos iniciales de las clases: 

```
handler.response
handler.JSONdata
handler.hierarchies
handler.data
handler.measures
handler.metainfo
handler.id_consulta
```

2. Obtener elementos de la respuesta:

```
dict_response = handler.get_elements_of_response()
dict_response
```

![visualización de dict_response](./imgs/dict_response.jpg)

```
dict_response["id_consulta"]
# 44804
```

3. Consulta de datos que devuelve BADEA tras la petición (handler.data), en formato DataFrame:

```
dataset = handler.get_DataFrame_dataJSON()
```

![visualización de datos en formato DataFrame](./imgs/dataset_df.jpg)

```
handler.df_data
```

![visualización a través de atributos](./imgs/atributo_df_data.jpg)

4. Hacer petición a "url" de información de una de las jerarquías:

```
hier = handler.hierarchies[0]
values_hier = handler.request_hierarchies_values(hier)
```

![Selección de una jerarquía](./imgs/una_jerarquia.jpg)

![Consulta sobre esa jerarquía](./imgs/values_hier.jpg)

5. Procesa las jerarquías llamando al método `process_all_hierarchies`:

```
processed_data = handler.process_all_hierarchies()
```

![Jerarquias procesadas](./imgs/jerarquias_procesadas.jpg)

Guardar el dataframe resultante con `.to_excel()`

```
cleaned_data.to_excel("jerarquias_limpiadas.xlsx")
```
