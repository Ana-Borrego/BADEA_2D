# Pruebas Unitarias de API con `unittest`

Este archivo documenta las pruebas realizadas en el archivo `test_api_data_handler.py` utilizando el framework `unittest`. En estas pruebas, se ejecuta un test real de consulta de API que valida la correcta ejecución del procesamiento de datos de la API y su transformación en un DataFrame.

## Ejecución de las pruebas

Para ejecutar las pruebas, abre una terminal en el directorio raíz del proyecto y utiliza el siguiente comando:

```bash
python -m unittest discover -s tests -p "test_api_data_handler.py"
```

El report realizado por unittest se puede observar en la ruta: `BADEA_2D/test-reports/`

**Descripción** 

+ `unittest discover`: Descubre y ejecuta las pruebas en los archivos de test.
+ `-s tests`: Especifica que el directorio tests/ es donde se encuentran los archivos de test.
+ `-p "test_api_data_handler.py"`: Indica que solo se debe ejecutar el archivo test_api_data_handler.py.

## Resultado esperado. 

```bash
Ran 1 test in 33.734s

OK
```

Este mensaje indica que la prueba se ejecutó correctamente y pasó sin errores. El tiempo de ejecución de la prueba puede variar según la velocidad de la consulta a la API y el procesamiento de los datos.

## Detalles de la prueba realizada. 

La prueba que se realiza en test_api_data_handler.py está diseñada para comprobar que la funcionalidad de la clase APIDataHandler procesa correctamente los datos de la API y los convierte en un DataFrame en formato adecuado.

+ **URL de la API**: La prueba consulta una API real de la Junta de Andalucía.
+ **Parámetros de consulta**: Los parámetros utilizados para realizar la consulta a la API son los siguientes:

```python
params = {
    "D_TEMPORAL_0" : "180194",
    "AA_TERRITROIO_0" : "515892",
    "D_SEXO_0" : "3691,3689,3690",
    "posord" : "f[D_AA_TERRITROIO_0],f[D_TEMPORAL_0],f[D_SEXO_0],f[D_EDAD_0],f[D_AA_DURAULTEMPR_0],c[Measures]"
}
```

+ **URL de la consulta**: 

```plaintext
https://www.juntadeandalucia.es/institutodeestadisticaycartografia/intranet/admin/rest/v1.0/consulta/44804?
```

+ **Función de la prueba**: La prueba valida que los datos recibidos de la API son procesados correctamente y convertidos en un DataFrame con las columnas correspondientes.

## Descripción del archivo de prueba `test_api_data_handler.py`: 

El archivo `test_api_data_handler.py` contiene una serie de tests que verifican que el método de la clase `APIDataHandler` funcione correctamente. En particular, el test principal (`test_real_api_processing`) realiza una consulta real a la API, procesa los datos y valida que la transformación en DataFrame sea correcta.

### ¿Qué hace el código test? 

+ **Configuración**: El método setUp() configura una instancia de APIDataHandler antes de cada test.
+ **Ejecución del test**: El método test_real_api_processing() consulta la API y procesa los datos, verificando que los datos estén en un DataFrame y que contenga las columnas necesarias.
+ **Verificación**: Se verifica que la respuesta sea un DataFrame de pandas y que contenga las columnas clave de los datos.