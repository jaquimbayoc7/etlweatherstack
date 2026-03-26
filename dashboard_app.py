#!/usr/bin/env python3
"""
dashboard_app.py — Dashboard básico de clima con Streamlit + PostgreSQL.
Ejecutar: streamlit run dashboard_app.py
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import sys
sys.path.insert(0, '.')

from scripts.database import SessionLocal, DB_HOST
from scripts.models import Ciudad, RegistroClima

# ── Configuración de página ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard de Clima ETL",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

if DB_HOST == "localhost":
    st.error(
        "🔐 **Base de datos no configurada.**\n\n"
        "Para desplegar esta app en Streamlit Cloud, agrega los secrets de la BD:\n\n"
        "1. Ve a tu app en share.streamlit.io\n"
        "2. Menú ⋮ → **Settings → Secrets**\n"
        "3. Pega el contenido de tu `.streamlit/secrets.toml`\n"
        "4. Guarda y reinicia la app"
    )
    st.stop()

st.title("🌍 Dashboard de Clima — ETL Weatherstack")
st.markdown("---")

# ── Conexión a BD ──────────────────────────────────────────────────────────────
db = SessionLocal()

try:
    # Obtiene todos los registros junto con el nombre de la ciudad
    registros = (
        db.query(RegistroClima, Ciudad.nombre)
        .join(Ciudad)
        .order_by(RegistroClima.fecha_extraccion.desc())
        .all()
    )

    if not registros:
        st.warning("⚠️ No hay datos en la base de datos. Ejecuta primero extractor_db.py")
        st.stop()

    # Construye DataFrame
    data = []
    for registro, ciudad_nombre in registros:
        data.append({
            "Ciudad":       ciudad_nombre,
            "Temperatura":  registro.temperatura,
            "Sensación":    registro.sensacion_termica,
            "Humedad":      registro.humedad,
            "Viento":       registro.velocidad_viento,
            "Descripción":  registro.descripcion,
            "Fecha":        registro.fecha_extraccion,
        })

    df = pd.DataFrame(data)

    # ── Sidebar — Filtros ──────────────────────────────────────────────────────
    st.sidebar.title("🔧 Filtros")
    ciudades_filtro = st.sidebar.multiselect(
        "Selecciona Ciudades:",
        options=sorted(df["Ciudad"].unique()),
        default=sorted(df["Ciudad"].unique()),
    )

    df_filtrado = df[df["Ciudad"].isin(ciudades_filtro)]

    if df_filtrado.empty:
        st.warning("⚠️ No hay datos para los filtros seleccionados.")
        st.stop()

    # ── Métricas principales ───────────────────────────────────────────────────
    st.subheader("📈 Métricas Principales")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        temp_promedio = df_filtrado["Temperatura"].mean()
        st.metric("🌡️ Temperatura Promedio", f"{temp_promedio:.1f} °C")

    with col2:
        humedad_promedio = df_filtrado["Humedad"].mean()
        st.metric("💧 Humedad Promedio", f"{humedad_promedio:.1f} %")

    with col3:
        viento_maximo = df_filtrado["Viento"].max()
        ciudad_viento = df_filtrado[df_filtrado["Viento"] == viento_maximo]["Ciudad"].values[0]
        st.metric("💨 Viento Máximo", f"{viento_maximo:.0f} km/h", delta=ciudad_viento)

    with col4:
        total_registros = len(df_filtrado)
        st.metric("📊 Total Registros", f"{total_registros:,}")

    st.markdown("---")

    # ── Gráficas ───────────────────────────────────────────────────────────────
    st.subheader("📉 Visualizaciones")
    col1, col2 = st.columns(2)

    # Temperatura promedio por ciudad (barras)
    with col1:
        temp_ciudad = df_filtrado.groupby("Ciudad")["Temperatura"].mean().reset_index()
        fig_temp = px.bar(
            temp_ciudad,
            x="Ciudad",
            y="Temperatura",
            title="🌡️ Temperatura Promedio por Ciudad",
            color="Temperatura",
            color_continuous_scale="RdYlBu_r",
            text=temp_ciudad["Temperatura"].round(1),
        )
        fig_temp.update_traces(texttemplate="%{text} °C", textposition="outside")
        fig_temp.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig_temp, width="stretch")

    # Humedad promedio por ciudad (barras)
    with col2:
        humid_ciudad = df_filtrado.groupby("Ciudad")["Humedad"].mean().reset_index()
        fig_humid = px.bar(
            humid_ciudad,
            x="Ciudad",
            y="Humedad",
            title="💧 Humedad Promedio por Ciudad",
            color="Humedad",
            color_continuous_scale="Blues",
            text=humid_ciudad["Humedad"].round(1),
        )
        fig_humid.update_traces(texttemplate="%{text} %", textposition="outside")
        fig_humid.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig_humid, width="stretch")

    col1, col2 = st.columns(2)

    # Scatter Temperatura vs Humedad
    with col1:
        fig_scatter = px.scatter(
            df_filtrado,
            x="Temperatura",
            y="Humedad",
            color="Ciudad",
            title="🔬 Temperatura vs Humedad",
            hover_data=["Descripción", "Fecha"],
            size_max=10,
        )
        st.plotly_chart(fig_scatter, width="stretch")

    # Velocidad del viento por ciudad
    with col2:
        viento_ciudad = df_filtrado.groupby("Ciudad")["Viento"].mean().reset_index()
        fig_wind = px.bar(
            viento_ciudad,
            x="Viento",
            y="Ciudad",
            orientation="h",
            title="💨 Viento Promedio por Ciudad (km/h)",
            color="Viento",
            color_continuous_scale="Teal",
            text=viento_ciudad["Viento"].round(1),
        )
        fig_wind.update_traces(texttemplate="%{text} km/h", textposition="outside")
        fig_wind.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig_wind, width="stretch")

    st.markdown("---")

    # ── Tabla de datos ─────────────────────────────────────────────────────────
    st.subheader("📋 Datos Detallados")
    st.dataframe(
        df_filtrado.sort_values("Fecha", ascending=False).reset_index(drop=True),
        width="stretch",
        height=400,
    )

finally:
    db.close()
