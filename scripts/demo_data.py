#!/usr/bin/env python3
"""
demo_data.py - Genera 1000 registros de clima simulados con variación horaria.
Simula lecturas cada ~30 minutos durante ~8 días para 5 ciudades colombianas.
"""

import json
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

random = np.random.default_rng(seed=42)

CIUDADES = [
    {
        "ciudad": "Bogotá",      "pais": "Colombia", "region": "Bogota D.C.",
        "latitud": "4.711",     "longitud": "-74.0721",
        "temp_base": 14,        "humedad_base": 80,
        "viento_base": 15,      "presion_base": 750,
    },
    {
        "ciudad": "Medellín",   "pais": "Colombia", "region": "Antioquia",
        "latitud": "6.2442",   "longitud": "-75.5812",
        "temp_base": 22,        "humedad_base": 68,
        "viento_base": 10,      "presion_base": 890,
    },
    {
        "ciudad": "Cali",       "pais": "Colombia", "region": "Valle del Cauca",
        "latitud": "3.4516",   "longitud": "-76.532",
        "temp_base": 28,        "humedad_base": 72,
        "viento_base": 12,      "presion_base": 880,
    },
    {
        "ciudad": "Barranquilla", "pais": "Colombia", "region": "Atlántico",
        "latitud": "10.9685",  "longitud": "-74.7813",
        "temp_base": 33,        "humedad_base": 79,
        "viento_base": 22,      "presion_base": 1011,
    },
    {
        "ciudad": "Cartagena",  "pais": "Colombia", "region": "Bolívar",
        "latitud": "10.391",   "longitud": "-75.4794",
        "temp_base": 31,        "humedad_base": 84,
        "viento_base": 18,      "presion_base": 1012,
    },
]

DESCRIPCIONES = [
    "Sunny", "Partly cloudy", "Overcast", "Light rain",
    "Moderate rain", "Clear", "Foggy", "Thunderstorm",
]

DIRECCIONES = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]


def generar_registros(n_total: int = 1000) -> list:
    registros = []
    registros_por_ciudad = n_total // len(CIUDADES)
    # Intervalo: cada 30 minutos, arrancando 8 días atrás
    inicio = datetime.now() - timedelta(days=registros_por_ciudad * 30 / 1440)

    for ciudad in CIUDADES:
        for i in range(registros_por_ciudad):
            hora = inicio + timedelta(minutes=i * 30)
            # Ciclo diurno: más calor al mediodía, más frío en la madrugada
            ciclo = np.sin((hora.hour - 6) * np.pi / 12)
            temp = round(ciudad["temp_base"] + ciclo * 4 + random.normal(0, 1.5), 1)
            humedad = int(np.clip(ciudad["humedad_base"] - ciclo * 8 + random.normal(0, 3), 30, 100))
            viento = round(max(0, ciudad["viento_base"] + random.normal(0, 3)), 1)
            presion = int(ciudad["presion_base"] + random.normal(0, 2))
            sensacion = round(temp - random.uniform(0, 3), 1)
            visibilidad = int(np.clip(random.normal(12, 3), 2, 20))
            uv = int(np.clip(ciclo * 8 + random.normal(0, 1), 0, 11))

            registros.append({
                "ciudad":           ciudad["ciudad"],
                "pais":             ciudad["pais"],
                "region":           ciudad["region"],
                "latitud":          ciudad["latitud"],
                "longitud":         ciudad["longitud"],
                "temperatura":      temp,
                "sensacion_termica": sensacion,
                "humedad":          humedad,
                "velocidad_viento": viento,
                "direccion_viento": random.choice(DIRECCIONES),
                "presion":          presion,
                "visibilidad":      visibilidad,
                "indice_uv":        uv,
                "descripcion":      random.choice(DESCRIPCIONES),
                "hora_local":       hora.strftime("%Y-%m-%d %H:%M"),
                "timestamp":        hora.isoformat(),
            })

    # Si n_total no es divisible exacto, completamos con los últimos registros
    while len(registros) < n_total:
        registros.append(registros[-1].copy())

    return registros[:n_total]


def generar_datos_demo(n: int = 1000):
    os.makedirs('data', exist_ok=True)
    datos = generar_registros(n)

    with open('data/clima_raw.json', 'w', encoding='utf-8') as f:
        json.dump(datos, f, indent=2, ensure_ascii=False)
    print(f"✅ data/clima_raw.json  — {len(datos)} registros")

    df = pd.DataFrame(datos)
    df.to_csv('data/clima.csv', index=False)
    print(f"✅ data/clima.csv       — {len(df)} filas x {len(df.columns)} columnas")

    print("\n📊 Resumen por ciudad:")
    resumen = df.groupby('ciudad').agg(
        registros=('temperatura', 'count'),
        temp_media=('temperatura', 'mean'),
        temp_min=('temperatura', 'min'),
        temp_max=('temperatura', 'max'),
        humedad_media=('humedad', 'mean'),
    ).round(1)
    print(resumen.to_string())
    print(f"\n⚠️  Datos de DEMO (simulados). Total: {len(df)} registros.")


if __name__ == "__main__":
    generar_datos_demo(1000)
