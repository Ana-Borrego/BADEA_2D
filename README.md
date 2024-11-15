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

------------------------------------------------------------------------

# 0. Instrucciones para la instalación. 

1. **Clona el repositorio:** 

```
git clone https://github.com/Ana-Borrego/BADEA_2D.git
```

2. **Crea un entorno virtual** (opcional pero recomendado)
    
    Si deseas evitar conflictos con las dependencias de otros proyectos, es recomendable crear un entorno virtual. Usa los siguientes comandos según tu sistema operativo:

    + Para sistemas basados en Unix (Linux/macOS):

```bash
python3 -m venv venv
source venv/bin/activate
```
    + Para Windows:

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

## 2. Flujo de trabajo. 

### 2.1. Inicialización.

Se crea una instancia de la clase APIDataHandler pasando la respuesta de la API. El constructor (`__init__`) extrae la información clave y la almacena en atributos de la clase. 

Para crear una instancia de la clase se precisa de proporcionarle la url a la tabla de IECA que se quiere obtener por consulta HTTPS desde la API de BADEA, y los parámetros, en forma de diccionario `params`, sobre el filtrado que se desea consultar. 

### 2.2. Obtener elementos de la respuesta. 

Se pueden obtener los elementos clave de la respuesta usando el método `.get_elements_of_response`, que devuelve un diccionario con las jerarquías, medidas, metainformación, etc.

### 2.3. Transformación de la clave de respuesta `"data"` a `DataFrame` de pandas. 

El método `.get_DataFrame_dataJSON` toma los datos de la respuesta y los convierte en un DataFrame de Pandas. Este DataFrame incluye tanto los datos estructurados como los códigos de las jerarquías, gracias a un proceso de extracción interno de los mismos. *Las columnas de código tendrán como sufijo `_cod`*. 

### 2.4. Procesar jerarquías. 

El método `.process_all_hierarchies` procesa todas las jerarquías, creando combinaciones de códigos y descripciones para el mapeo o la visualización de los valores posibles de los parámetros de consulta (correspondientes a la integración del valor `id` (siguiendo formato "{id_1},{id_2},{id_3}") en el parámetro de `params` deseado). También limpia la columna COD_combination eliminando valores no deseados para facilitar el mapeo. 

El resultado puede ser descargado en el formato deseado, y/o consultado mediante el atributo `self.hierarchies_info_df`, para la generación de nuevas consultas. Además el resultado es utilizado en el método final de aplanamiento en 2 dimensiones de los datos de consulta devueltos por BADEA. 

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
