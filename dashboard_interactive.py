#!/usr/bin/env python3
"""
dashboard_interactive.py — Dashboard interactivo con control total de filtros.
Ejecutar: streamlit run dashboard_interactive.py
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from sqlalchemy import func, and_
import sys
sys.path.insert(0, '.')

from scripts.database import SessionLocal, DB_HOST
from scripts.models import Ciudad, RegistroClima

# ── Configuración de página ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard Interactivo",
    page_icon="🎛️",
    layout="wide",
)

# CSS personalizado
st.markdown(
    """
    <style>
    .metric-box {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .stMetric {
        background-color: #f8f9fa;
        border-radius: 8px;
        padding: 12px;
        border-left: 4px solid #2a5298;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🎛️ Dashboard Interactivo — Control Total")

# Verifica que las credenciales de BD estén configuradas
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

db = SessionLocal()

# ── Sidebar — Controles ────────────────────────────────────────────────────────
st.sidebar.markdown("### 🔧 Controles")

# Ciudades disponibles
ciudades_disponibles = [c.nombre for c in db.query(Ciudad).order_by(Ciudad.nombre).all()]

ciudades_seleccionadas = st.sidebar.multiselect(
    "🏙️ Ciudades a Mostrar",
    options=ciudades_disponibles,
    default=ciudades_disponibles,
)

# Rango de fechas
st.sidebar.markdown("**📅 Rango de Fechas**")
col1, col2 = st.sidebar.columns(2)
with col1:
    fecha_inicio = st.sidebar.date_input(
        "Desde:",
        value=datetime.now() - timedelta(days=30),
    )
with col2:
    fecha_fin = st.sidebar.date_input(
        "Hasta:",
        value=datetime.now(),
    )

# Filtros de temperatura
st.sidebar.markdown("**🌡️ Rango de Temperatura**")

# Determina el rango real del dataset
temp_global_min_row = db.query(func.min(RegistroClima.temperatura)).scalar() or -10
temp_global_max_row = db.query(func.max(RegistroClima.temperatura)).scalar() or 50
t_min = int(temp_global_min_row) - 1
t_max = int(temp_global_max_row) + 1

temp_rango = st.sidebar.slider(
    "Temperatura (°C):",
    min_value=t_min,
    max_value=t_max,
    value=(t_min, t_max),
)
temp_min, temp_max = temp_rango

# ── Consulta filtrada ──────────────────────────────────────────────────────────
registros_filtrados = (
    db.query(RegistroClima, Ciudad.nombre, Ciudad.pais)
    .join(Ciudad)
    .filter(
        and_(
            Ciudad.nombre.in_(ciudades_seleccionadas) if ciudades_seleccionadas else True,
            RegistroClima.fecha_extraccion >= fecha_inicio,
            RegistroClima.fecha_extraccion <= fecha_fin,
            RegistroClima.temperatura >= temp_min,
            RegistroClima.temperatura <= temp_max,
        )
    )
    .order_by(RegistroClima.fecha_extraccion)
    .all()
)

# Construye DataFrame
data = []
for registro, ciudad_nombre, pais in registros_filtrados:
    data.append(
        {
            "Ciudad":      ciudad_nombre,
            "País":        pais,
            "Temperatura": registro.temperatura,
            "Sensación":   registro.sensacion_termica,
            "Humedad":     registro.humedad,
            "Viento":      registro.velocidad_viento,
            "Descripción": registro.descripcion,
            "Fecha":       registro.fecha_extraccion,
        }
    )

df = pd.DataFrame(data) if data else pd.DataFrame()

# ── Cuerpo principal ───────────────────────────────────────────────────────────
if not df.empty:
    df["Fecha"] = pd.to_datetime(df["Fecha"])

    # KPIs
    st.markdown("### 📊 Indicadores Clave")
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("📊 Registros", f"{len(df):,}")

    with col2:
        st.metric("🌡️ Temp Promedio", f"{df['Temperatura'].mean():.1f} °C")

    with col3:
        st.metric(
            "🔺 Temp Máxima",
            f"{df['Temperatura'].max():.1f} °C",
            delta=df.loc[df['Temperatura'].idxmax(), 'Ciudad'],
        )

    with col4:
        st.metric("💧 Humedad Prom.", f"{df['Humedad'].mean():.1f} %")

    with col5:
        st.metric(
            "💨 Viento Máx.",
            f"{df['Viento'].max():.0f} km/h",
            delta=df.loc[df['Viento'].idxmax(), 'Ciudad'],
        )

    st.markdown("---")

    # ── Gráficas ───────────────────────────────────────────────────────────────
    col1, col2 = st.columns(2)

    # Boxplot de temperatura por ciudad
    with col1:
        st.markdown("#### 📦 Distribución de Temperaturas")
        fig = px.box(
            df,
            x="Ciudad",
            y="Temperatura",
            color="Ciudad",
            title="Comparativa de Temperaturas por Ciudad",
            points="outliers",
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, width="stretch")

    # Barras de humedad promedio
    with col2:
        st.markdown("#### 💧 Humedad Promedio")
        humedad_ciudad = df.groupby("Ciudad")["Humedad"].mean().reset_index()
        fig = px.bar(
            humedad_ciudad,
            x="Ciudad",
            y="Humedad",
            color="Humedad",
            title="Humedad Promedio por Ciudad",
            color_continuous_scale="Blues",
            text=humedad_ciudad["Humedad"].round(1),
        )
        fig.update_traces(texttemplate="%{text} %", textposition="outside")
        fig.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig, width="stretch")

    st.markdown("---")

    # Línea temporal
    st.markdown("#### 📈 Evolución Temporal de Temperatura")
    temp_tiempo = (
        df.groupby([pd.Grouper(key="Fecha", freq="6h"), "Ciudad"])["Temperatura"]
        .mean()
        .reset_index()
    )
    fig = px.line(
        temp_tiempo,
        x="Fecha",
        y="Temperatura",
        color="Ciudad",
        title="Temperatura Promedio cada 6 horas",
        markers=False,
        line_shape="spline",
    )
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, width="stretch")

    # Scatter Temperatura vs Humedad
    st.markdown("#### 🔬 Temperatura vs Humedad")
    fig = px.scatter(
        df,
        x="Temperatura",
        y="Humedad",
        color="Ciudad",
        size="Viento",
        hover_data=["Descripción", "Fecha"],
        title="Temperatura vs Humedad (tamaño = velocidad del viento)",
        opacity=0.7,
    )
    st.plotly_chart(fig, width="stretch")

    st.markdown("---")

    # ── Tabla interactiva ──────────────────────────────────────────────────────
    st.markdown("#### 📋 Datos Detallados")

    col1, col2 = st.columns(2)
    with col1:
        mostrar_todos = st.checkbox("Mostrar todos los registros", value=False)
    with col2:
        columnas_mostrar = st.multiselect(
            "Columnas a mostrar:",
            options=list(df.columns),
            default=["Ciudad", "Temperatura", "Humedad", "Viento", "Descripción", "Fecha"],
        )

    df_vista = df[columnas_mostrar] if columnas_mostrar else df
    if mostrar_todos:
        st.dataframe(df_vista.sort_values("Fecha", ascending=False), width="stretch", height=600)
    else:
        st.dataframe(
            df_vista.sort_values("Fecha", ascending=False).head(100),
            width="stretch",
            height=400,
        )
        st.caption(f"Mostrando 100 de {len(df):,} registros. Activa 'Mostrar todos' para ver el dataset completo.")

    # ── Descarga ───────────────────────────────────────────────────────────────
    st.markdown("---")
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Descargar datos filtrados (CSV)",
        data=csv,
        file_name=f"clima_filtrado_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
    )

else:
    st.warning("⚠️ No hay datos que coincidan con los filtros seleccionados. Ajusta los controles del panel lateral.")

db.close()
