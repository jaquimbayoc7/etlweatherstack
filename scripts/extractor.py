#!/usr/bin/env python3
"""
extractor.py - Fase Extract del pipeline ETL
Consume la API de Weatherstack y guarda los datos en CSV y JSON.
"""

import os
import requests
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import logging

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class WeatherstackExtractor:
    def __init__(self):
        self.api_key = os.getenv('API_KEY')
        self.base_url = os.getenv('WEATHERSTACK_BASE_URL')
        self.ciudades = os.getenv('CIUDADES').split(',')

        if not self.api_key:
            raise ValueError("API_KEY no configurada. Verifica tu archivo .env")

    def extraer_clima(self, ciudad):
        """Extrae datos de clima para una ciudad específica"""
        try:
            params = {
                'access_key': self.api_key,
                'query': ciudad,
                'units': 'm'  # métrico (°C, km/h)
            }
            response = requests.get(
                f"{self.base_url}/current",
                params=params,
                timeout=10
            )
            # Leer siempre el JSON antes de verificar errores HTTP
            data = response.json()

            if not response.ok or 'error' in data:
                error_info = data.get('error', {})
                code = error_info.get('code', response.status_code)
                info = error_info.get('info', 'Error desconocido')
                logger.error(f"Error API [{code}] para {ciudad}: {info}")
                if code == 615:
                    logger.warning(
                        "Código 615: Verifica que tu cuenta Weatherstack esté "
                        "activa y el email confirmado en weatherstack.com"
                    )
                return None

            return data

        except requests.exceptions.Timeout:
            logger.error(f"Timeout al conectar con la API para {ciudad}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error de red para {ciudad}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error inesperado extrayendo {ciudad}: {str(e)}")
            return None

    def procesar_respuesta(self, response_data):
        """Procesa la respuesta JSON a formato estructurado"""
        try:
            current = response_data['current']
            location = response_data['location']

            return {
                'ciudad': location['name'],
                'pais': location['country'],
                'region': location.get('region', ''),
                'latitud': location.get('lat', ''),
                'longitud': location.get('lon', ''),
                'temperatura': current['temperature'],
                'sensacion_termica': current['feelslike'],
                'humedad': current['humidity'],
                'velocidad_viento': current['wind_speed'],
                'direccion_viento': current['wind_dir'],
                'presion': current['pressure'],
                'visibilidad': current['visibility'],
                'indice_uv': current.get('uv_index', 0),
                'descripcion': (
                    current['weather_descriptions'][0]
                    if current.get('weather_descriptions') else ''
                ),
                'hora_local': location['localtime'],
                'timestamp': datetime.now().isoformat()
            }

        except KeyError as e:
            logger.error(f"Campo faltante en la respuesta: {e}")
            return None
        except Exception as e:
            logger.error(f"Error procesando respuesta: {str(e)}")
            return None

    def ejecutar_extraccion(self):
        """Ejecuta la extracción para todas las ciudades"""
        datos_extraidos = []

        logger.info(f"Iniciando extracción para {len(self.ciudades)} ciudades...")

        for ciudad in self.ciudades:
            ciudad = ciudad.strip()
            logger.info(f"Extrayendo datos para {ciudad}...")
            import time
            time.sleep(1)  # Evitar rate limiting (máx 5 req/s en plan gratuito)
            raw_data = self.extraer_clima(ciudad)

            if raw_data:
                datos = self.procesar_respuesta(raw_data)
                if datos:
                    datos_extraidos.append(datos)
                    logger.info(
                        f"✅ {datos['ciudad']}: {datos['temperatura']}°C, "
                        f"Humedad: {datos['humedad']}%"
                    )
            else:
                logger.warning(f"⚠️  No se pudieron obtener datos para {ciudad}")

        logger.info(
            f"Extracción completada: {len(datos_extraidos)}/{len(self.ciudades)} ciudades exitosas"
        )
        return datos_extraidos


if __name__ == "__main__":
    try:
        extractor = WeatherstackExtractor()
        datos = extractor.ejecutar_extraccion()

        if not datos:
            logger.error("No se extrajeron datos. Verifica la API key y la conexión.")
            exit(1)

        # Guardar como JSON
        os.makedirs('data', exist_ok=True)
        with open('data/clima_raw.json', 'w', encoding='utf-8') as f:
            json.dump(datos, f, indent=2, ensure_ascii=False)
        logger.info("📁 Datos guardados en data/clima_raw.json")

        # Guardar como CSV
        df = pd.DataFrame(datos)
        df.to_csv('data/clima.csv', index=False)
        logger.info("📁 Datos guardados en data/clima.csv")

        print("\n" + "=" * 60)
        print("RESUMEN DE EXTRACCIÓN")
        print("=" * 60)
        print(df[['ciudad', 'temperatura', 'sensacion_termica',
                   'humedad', 'velocidad_viento', 'descripcion']].to_string(index=False))
        print("=" * 60)

    except Exception as e:
        logger.error(f"Error fatal en extracción: {str(e)}")
        raise
