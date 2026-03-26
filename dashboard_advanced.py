#!/usr/bin/env python3
"""
dashboard_advanced.py — Dashboard avanzado con pestañas y análisis completo.
Ejecutar: streamlit run dashboard_advanced.py
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from sqlalchemy import func
import sys
sys.path.insert(0, '.')

from scripts.database import SessionLocal
from scripts.models import Ciudad, RegistroClima, MetricasETL

# ── Configuración de página ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard Avanzado Clima",
    page_icon="🌡️",
    layout="wide",
)

st.title("🌍 Dashboard Avanzado — Análisis de Clima")
st.markdown("---")

db = SessionLocal()

# ── Pestañas principales ───────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(
    ["📊 Vista General", "📈 Histórico", "🔍 Análisis", "📋 Métricas ETL"]
)

# ══════════════════════════════════════════════════════════
# TAB 1 — Vista General
# ══════════════════════════════════════════════════════════
with tab1:
    st.subheader("Datos Actuales")

    col1, col2, col3 = st.columns(3)

    with col1:
        ciudades_count = db.query(func.count(Ciudad.id)).scalar()
        st.metric("🏙️ Ciudades", ciudades_count)

    with col2:
        registros_count = db.query(func.count(RegistroClima.id)).scalar()
        st.metric("📊 Registros Totales", f"{registros_count:,}")

    with col3:
        ultima_fecha = db.query(func.max(RegistroClima.fecha_extraccion)).scalar()
        if ultima_fecha:
            st.metric("⏰ Última Actualización", ultima_fecha.strftime("%Y-%m-%d %H:%M"))

    st.markdown("---")

    # Último registro por ciudad (DISTINCT ON ciudad_id)
    subq = (
        db.query(
            RegistroClima.ciudad_id,
            func.max(RegistroClima.fecha_extraccion).label("max_fecha"),
        )
        .group_by(RegistroClima.ciudad_id)
        .subquery()
    )

    registros_actuales = (
        db.query(
            Ciudad.nombre,
            RegistroClima.temperatura,
            RegistroClima.humedad,
            RegistroClima.velocidad_viento,
            RegistroClima.descripcion,
        )
        .join(RegistroClima, Ciudad.id == RegistroClima.ciudad_id)
        .join(
            subq,
            (RegistroClima.ciudad_id == subq.c.ciudad_id)
            & (RegistroClima.fecha_extraccion == subq.c.max_fecha),
        )
        .all()
    )

    df_actual = pd.DataFrame(
        registros_actuales,
        columns=["Ciudad", "Temperatura", "Humedad", "Viento", "Descripción"],
    )

    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(
            df_actual,
            x="Ciudad",
            y="Temperatura",
            title="🌡️ Temperatura Actual por Ciudad",
            color="Temperatura",
            color_continuous_scale="RdYlBu_r",
            text=df_actual["Temperatura"].round(1),
        )
        fig.update_traces(texttemplate="%{text} °C", textposition="outside")
        fig.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig, width="stretch")

    with col2:
        fig = px.pie(
            df_actual,
            values="Humedad",
            names="Ciudad",
            title="💧 Distribución de Humedad",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        st.plotly_chart(fig, width="stretch")

    st.markdown("---")
    st.dataframe(df_actual, width="stretch")

# ══════════════════════════════════════════════════════════
# TAB 2 — Histórico
# ══════════════════════════════════════════════════════════
with tab2:
    st.subheader("Análisis Histórico")

    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio = st.date_input(
            "Desde:", value=datetime.now() - timedelta(days=7)
        )
    with col2:
        fecha_fin = st.date_input("Hasta:", value=datetime.now())

    registros_historicos = (
        db.query(RegistroClima, Ciudad.nombre)
        .join(Ciudad)
        .filter(
            RegistroClima.fecha_extraccion >= fecha_inicio,
            RegistroClima.fecha_extraccion <= fecha_fin,
        )
        .order_by(RegistroClima.fecha_extraccion)
        .all()
    )

    if registros_historicos:
        data = []
        for registro, ciudad_nombre in registros_historicos:
            data.append(
                {
                    "Ciudad":      ciudad_nombre,
                    "Temperatura": registro.temperatura,
                    "Humedad":     registro.humedad,
                    "Viento":      registro.velocidad_viento,
                    "Descripción": registro.descripcion,
                    "Fecha":       registro.fecha_extraccion,
                }
            )

        df_historico = pd.DataFrame(data)

        # Línea de temperatura en el tiempo
        fig = px.line(
            df_historico,
            x="Fecha",
            y="Temperatura",
            color="Ciudad",
            title="📈 Evolución de Temperatura",
            markers=True,
        )
        st.plotly_chart(fig, width="stretch")

        # Línea de humedad en el tiempo
        fig2 = px.line(
            df_historico,
            x="Fecha",
            y="Humedad",
            color="Ciudad",
            title="💧 Evolución de Humedad",
            markers=True,
        )
        st.plotly_chart(fig2, width="stretch")

        st.markdown("---")
        st.dataframe(df_historico, width="stretch")
    else:
        st.warning("⚠️ No hay datos en ese rango de fechas.")

# ══════════════════════════════════════════════════════════
# TAB 3 — Análisis estadístico por ciudad
# ══════════════════════════════════════════════════════════
with tab3:
    st.subheader("Análisis Estadístico por Ciudad")

    ciudades = db.query(Ciudad).order_by(Ciudad.nombre).all()

    for ciudad in ciudades:
        registros_ciudad = (
            db.query(RegistroClima)
            .filter(RegistroClima.ciudad_id == ciudad.id)
            .all()
        )

        if not registros_ciudad:
            continue

        temps = [r.temperatura for r in registros_ciudad]
        humeds = [r.humedad for r in registros_ciudad]
        vientos = [r.velocidad_viento for r in registros_ciudad]

        with st.expander(f"📍 {ciudad.nombre}  ({len(registros_ciudad)} registros)"):
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("🌡️ Temp Prom",  f"{sum(temps)/len(temps):.1f} °C")
            c2.metric("🔻 Temp Mín",   f"{min(temps):.1f} °C")
            c3.metric("🔺 Temp Máx",   f"{max(temps):.1f} °C")
            c4.metric("💧 Humedad",    f"{sum(humeds)/len(humeds):.1f} %")
            c5.metric("💨 Viento Prom",f"{sum(vientos)/len(vientos):.1f} km/h")

            # Mini histograma de temperatura
            df_c = pd.DataFrame({"Temperatura": temps, "Humedad": humeds})
            fig = px.histogram(
                df_c, x="Temperatura", nbins=20,
                title=f"Distribución de Temperatura — {ciudad.nombre}",
                color_discrete_sequence=["#2a5298"],
            )
            fig.update_layout(height=250, margin=dict(t=40, b=20))
            st.plotly_chart(fig, width="stretch")

# ══════════════════════════════════════════════════════════
# TAB 4 — Métricas ETL
# ══════════════════════════════════════════════════════════
with tab4:
    st.subheader("Métricas de Ejecución ETL")

    metricas = (
        db.query(MetricasETL)
        .order_by(MetricasETL.fecha_ejecucion.desc())
        .limit(20)
        .all()
    )

    if metricas:
        data = []
        for m in metricas:
            data.append(
                {
                    "Fecha":          m.fecha_ejecucion.strftime("%Y-%m-%d %H:%M:%S"),
                    "Estado":         m.estado,
                    "Extraídos":      m.registros_extraidos,
                    "Guardados":      m.registros_guardados,
                    "Fallidos":       m.registros_fallidos,
                    "Tiempo (s)":     round(m.tiempo_ejecucion_segundos, 2),
                    "Mensaje":        m.mensaje or "",
                }
            )

        df_metricas = pd.DataFrame(data)
        st.dataframe(df_metricas, width="stretch")

        st.markdown("---")
        col1, col2 = st.columns(2)

        with col1:
            fig = px.bar(
                df_metricas,
                x="Fecha",
                y=["Guardados", "Fallidos"],
                title="📊 Registros por Ejecución",
                barmode="group",
                color_discrete_sequence=["#27ae60", "#e74c3c"],
            )
            st.plotly_chart(fig, width="stretch")

        with col2:
            fig = px.line(
                df_metricas,
                x="Fecha",
                y="Tiempo (s)",
                title="⏱️ Tiempo de Ejecución (s)",
                markers=True,
                color_discrete_sequence=["#2a5298"],
            )
            st.plotly_chart(fig, width="stretch")
    else:
        st.info("ℹ️ No hay métricas registradas aún. Ejecuta extractor_db.py primero.")

db.close()
