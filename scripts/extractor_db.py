#!/usr/bin/env python3
"""
extractor_db.py — Fase Extract del ETL con persistencia en PostgreSQL.
Lee datos de clima_transformado.csv y los inserta en la base de datos.
"""
import sys
sys.path.insert(0, '.')

import os
import pandas as pd
import time
from datetime import datetime
from dotenv import load_dotenv
import logging
from sqlalchemy.exc import IntegrityError

from scripts.database import SessionLocal, create_all_tables
from scripts.models import Ciudad, RegistroClima, MetricasETL
import scripts.models  # noqa: F401

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Coordenadas aproximadas de las ciudades colombianas
COORDENADAS = {
    'Bogota':       {'pais': 'Colombia', 'lat': 4.711,   'lon': -74.0721},
    'Medellin':     {'pais': 'Colombia', 'lat': 6.2442,  'lon': -75.5812},
    'Cali':         {'pais': 'Colombia', 'lat': 3.4516,  'lon': -76.5320},
    'Barranquilla': {'pais': 'Colombia', 'lat': 10.9685, 'lon': -74.7813},
    'Cartagena':    {'pais': 'Colombia', 'lat': 10.3910, 'lon': -75.4794},
}


class WeatherstackETLDB:
    def __init__(self):
        create_all_tables()
        self.db = SessionLocal()
        self.tiempo_inicio = time.time()
        self.registros_extraidos = 0
        self.registros_guardados = 0
        self.registros_fallidos = 0

    # ------------------------------------------------------------------
    # Obtener o crear ciudad
    # ------------------------------------------------------------------
    def _obtener_ciudad(self, nombre: str) -> Ciudad:
        ciudad = self.db.query(Ciudad).filter(Ciudad.nombre == nombre).first()
        if not ciudad:
            coords = COORDENADAS.get(nombre, {'pais': 'Colombia', 'lat': None, 'lon': None})
            ciudad = Ciudad(
                nombre=nombre,
                pais=coords['pais'],
                latitud=coords['lat'],
                longitud=coords['lon'],
            )
            self.db.add(ciudad)
            self.db.commit()
            self.db.refresh(ciudad)
            logger.info(f"🏙️  Ciudad registrada: {nombre}")
        return ciudad

    # ------------------------------------------------------------------
    # Guardar un registro de clima
    # ------------------------------------------------------------------
    def _guardar_registro(self, ciudad: Ciudad, fila: pd.Series) -> bool:
        try:
            registro = RegistroClima(
                ciudad_id=ciudad.id,
                temperatura=float(fila.get('temperatura', 0)),
                sensacion_termica=float(fila.get('sensacion_termica', fila.get('temperatura', 0))),
                humedad=float(fila.get('humedad', 0)),
                velocidad_viento=float(fila.get('velocidad_viento', 0)),
                descripcion=str(fila.get('descripcion', 'N/A'))[:255],
                codigo_tiempo=int(fila['codigo_tiempo']) if 'codigo_tiempo' in fila and pd.notna(fila['codigo_tiempo']) else 113,
                fecha_extraccion=pd.to_datetime(fila.get('hora_local', datetime.utcnow())),
            )
            self.db.add(registro)
            self.db.commit()
            return True
        except IntegrityError:
            self.db.rollback()
            logger.warning(f"⚠️  Registro duplicado omitido para {ciudad.nombre}")
            return False
        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ Error guardando registro: {e}")
            return False

    # ------------------------------------------------------------------
    # Guardar métricas de ejecución
    # ------------------------------------------------------------------
    def _guardar_metricas(self, estado: str):
        try:
            tiempo = round(time.time() - self.tiempo_inicio, 2)
            metricas = MetricasETL(
                registros_extraidos=self.registros_extraidos,
                registros_guardados=self.registros_guardados,
                registros_fallidos=self.registros_fallidos,
                tiempo_ejecucion_segundos=tiempo,
                estado=estado,
                mensaje=(
                    f"{self.registros_guardados} registros guardados de "
                    f"{self.registros_extraidos} en {tiempo}s"
                ),
            )
            self.db.add(metricas)
            self.db.commit()
            logger.info(f"📈 Métricas guardadas — estado: {estado}")
        except Exception as e:
            logger.error(f"❌ Error guardando métricas: {e}")

    # ------------------------------------------------------------------
    # Ejecutar pipeline completo (bulk insert para máxima velocidad)
    # ------------------------------------------------------------------
    def ejecutar(self) -> bool:
        csv_path = 'data/clima_transformado.csv'
        if not os.path.exists(csv_path):
            logger.error(f"❌ No se encontró {csv_path}. Ejecuta primero transformador.py")
            return False

        logger.info(f"📂 Cargando datos desde {csv_path}")
        df = pd.read_csv(csv_path)
        self.registros_extraidos = len(df)
        logger.info(f"📊 {self.registros_extraidos} registros a procesar")

        # 1 ─ Registrar ciudades únicas de una vez
        ciudades_unicas = df['ciudad'].dropna().unique()
        ciudad_map = {}
        for nombre in ciudades_unicas:
            nombre = str(nombre).strip()
            ciudad_map[nombre] = self._obtener_ciudad(nombre)
        logger.info(f"🏙️  {len(ciudad_map)} ciudades registradas")

        # 2 ─ Construir todos los objetos en memoria
        registros_bulk = []
        for _, fila in df.iterrows():
            nombre_ciudad = str(fila.get('ciudad', '')).strip()
            ciudad = ciudad_map.get(nombre_ciudad)
            if not ciudad:
                self.registros_fallidos += 1
                continue
            try:
                # codigo_tiempo: usa la columna si existe, sino deriva un código
                # numérico desde la descripción del clima
                raw_codigo = fila.get('codigo_tiempo')
                if pd.notna(raw_codigo) and raw_codigo != '':
                    codigo_tiempo = int(raw_codigo)
                else:
                    # Mapeo básico descripción → código WMO
                    desc = str(fila.get('descripcion', '')).lower()
                    _DESC_MAP = {
                        'sunny': 113, 'clear': 113, 'despejado': 113,
                        'partly cloudy': 116, 'parcialmente': 116,
                        'cloudy': 119, 'nublado': 119, 'overcast': 122,
                        'fog': 143, 'foggy': 143, 'niebla': 143,
                        'drizzle': 266, 'llovizna': 266,
                        'rain': 296, 'lluvia': 296, 'rainy': 296,
                        'thunderstorm': 389, 'tormenta': 389,
                        'snow': 338, 'nieve': 338,
                    }
                    codigo_tiempo = next(
                        (v for k, v in _DESC_MAP.items() if k in desc), 113
                    )

                registros_bulk.append(RegistroClima(
                    ciudad_id=ciudad.id,
                    temperatura=float(fila.get('temperatura', 0)),
                    sensacion_termica=float(fila.get('sensacion_termica', fila.get('temperatura', 0))),
                    humedad=float(fila.get('humedad', 0)),
                    velocidad_viento=float(fila.get('velocidad_viento', 0)),
                    descripcion=str(fila.get('descripcion', 'N/A'))[:255],
                    codigo_tiempo=codigo_tiempo,
                    fecha_extraccion=pd.to_datetime(fila.get('hora_local', datetime.now())),
                ))
            except Exception as e:
                logger.warning(f"⚠️  Fila omitida: {e}")
                self.registros_fallidos += 1

        # 3 ─ Un único commit para todos los registros
        try:
            self.db.bulk_save_objects(registros_bulk)
            self.db.commit()
            self.registros_guardados = len(registros_bulk)
            logger.info(f"✅ Bulk insert completado: {self.registros_guardados} registros")
        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ Error en bulk insert: {e}")
            return False

        estado = 'SUCCESS' if self.registros_fallidos == 0 else 'PARTIAL'
        self._guardar_metricas(estado)

        logger.info(
            f"✅ ETL completado — Guardados: {self.registros_guardados} | "
            f"Fallidos: {self.registros_fallidos}"
        )
        return True

    # ------------------------------------------------------------------
    # Mostrar resumen de la BD
    # ------------------------------------------------------------------
    def mostrar_resumen(self):
        try:
            total = self.db.query(RegistroClima).count()
            ciudades = self.db.query(Ciudad).count()
            print(f"\n📊 RESUMEN EN BASE DE DATOS")
            print(f"   Ciudades registradas : {ciudades}")
            print(f"   Registros de clima   : {total}")
        except Exception as e:
            logger.error(f"❌ Error mostrando resumen: {e}")
        finally:
            self.db.close()


if __name__ == "__main__":
    etl = WeatherstackETLDB()
    exito = etl.ejecutar()
    etl.mostrar_resumen()
    sys.exit(0 if exito else 1)
