#!/usr/bin/env python3
"""
consultas.py — Análisis de datos almacenados en PostgreSQL.
"""
import sys
sys.path.insert(0, '.')

import pandas as pd
from sqlalchemy import func
from scripts.database import SessionLocal
from scripts.models import Ciudad, RegistroClima, MetricasETL
import scripts.models  # noqa: F401

db = SessionLocal()


def temperatura_promedio_por_ciudad():
    """Temperatura promedio de cada ciudad"""
    registros = db.query(
        Ciudad.nombre,
        func.avg(RegistroClima.temperatura).label('temp_promedio'),
        func.min(RegistroClima.temperatura).label('temp_min'),
        func.max(RegistroClima.temperatura).label('temp_max'),
    ).join(RegistroClima).group_by(Ciudad.nombre).all()

    df = pd.DataFrame(registros, columns=['Ciudad', 'Promedio °C', 'Mín °C', 'Máx °C'])
    df[['Promedio °C', 'Mín °C', 'Máx °C']] = df[['Promedio °C', 'Mín °C', 'Máx °C']].round(1)
    print("\n📊 TEMPERATURA POR CIUDAD:")
    print(df.to_string(index=False))


def ciudad_mas_humeda():
    """Identifica ciudad con mayor humedad promedio"""
    registros = db.query(
        Ciudad.nombre,
        func.avg(RegistroClima.humedad).label('humedad_promedio'),
    ).join(RegistroClima).group_by(Ciudad.nombre).order_by(
        func.avg(RegistroClima.humedad).desc()
    ).all()

    if registros:
        top = registros[0]
        print(f"\n💧 CIUDAD MÁS HÚMEDA: {top.nombre} con {top.humedad_promedio:.1f}% promedio")


def velocidad_viento_max():
    """Velocidad máxima de viento registrada"""
    registro = db.query(
        Ciudad.nombre,
        RegistroClima.velocidad_viento,
        RegistroClima.fecha_extraccion,
    ).join(Ciudad).order_by(
        RegistroClima.velocidad_viento.desc()
    ).first()

    if registro:
        print(
            f"\n💨 VIENTO MÁS FUERTE: {registro.nombre} "
            f"con {registro.velocidad_viento} km/h "
            f"({registro.fecha_extraccion})"
        )


def metricas_etl():
    """Muestra las últimas ejecuciones del ETL"""
    metricas = db.query(MetricasETL).order_by(
        MetricasETL.fecha_ejecucion.desc()
    ).limit(5).all()

    print("\n📈 ÚLTIMAS 5 EJECUCIONES DEL ETL:")
    if metricas:
        for m in metricas:
            print(
                f"  [{m.estado}] {m.fecha_ejecucion} — "
                f"{m.registros_guardados}/{m.registros_extraidos} registros "
                f"en {m.tiempo_ejecucion_segundos:.2f}s"
            )
    else:
        print("  (Sin ejecuciones registradas aún)")


if __name__ == "__main__":
    try:
        print("\n" + "=" * 50)
        print("ANÁLISIS DE DATOS — POSTGRESQL")
        print("=" * 50)

        temperatura_promedio_por_ciudad()
        ciudad_mas_humeda()
        velocidad_viento_max()
        metricas_etl()

        print("\n" + "=" * 50 + "\n")
    finally:
        db.close()
