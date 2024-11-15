import unittest
import requests
import sys 
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from src.main import APIDataHandler  # Cambia esto por el módulo real donde resides la clase

class TestAPIDataHandlerRealQuery(unittest.TestCase):

    def setUp(self):
        """Realiza una consulta real a la API y configura la clase."""
        self.url = "https://www.juntadeandalucia.es/institutodeestadisticaycartografia/intranet/admin/rest/v1.0/consulta/44804?"
        self.params = {
            "D_TEMPORAL_0": "180194",
            "AA_TERRITROIO_0": "515892",
            "D_SEXO_0": "3691,3689,3690",
            "posord": "f[D_AA_TERRITROIO_0],f[D_TEMPORAL_0],f[D_SEXO_0],f[D_EDAD_0],f[D_AA_DURAULTEMPR_0],c[Measures]"
        }

        # Realizar la solicitud a la API
        response = requests.get(self.url, params=self.params)
        self.assertEqual(response.status_code, 200, "La API no está disponible o la consulta falló.")

        # Inicializar la clase con la respuesta real
        self.handler = APIDataHandler(response)

    def test_real_api_processing(self):
        """Prueba el procesamiento de datos reales de la API."""
        # Procesar datos y jerarquías
        self.handler.get_DataFrame_dataJSON(process_measures=True)
        self.handler.process_all_hierarchies()
        self.handler.map_data_w_hierarchies_info()

        # Verificar que los datos mapeados contengan información esperada
        df_mapped = self.handler.df_data_mapped
        self.assertIsNotNone(df_mapped, "El DataFrame mapeado está vacío.")
        self.assertGreater(len(df_mapped), 0, "El DataFrame mapeado no contiene filas.")
        self.assertTrue("Des1" in df_mapped.columns, "La columna Des1 no está presente en el DataFrame mapeado.")

if __name__ == "__main__":
    unittest.main()
