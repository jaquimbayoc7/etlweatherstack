# ETL Weatherstack — Dashboard de clima

Dashboard interactivo que muestra datos históricos de clima para 5 ciudades colombianas (Bogotá, Medellín, Cali, Barranquilla, Cartagena), construido con una pipeline ETL completa.

## Tecnologías

| Capa | Herramienta |
|---|---|
| Extracción | Weatherstack API |
| Transformación | Python / Pandas |
| Almacenamiento | PostgreSQL (Supabase) |
| ORM / Migraciones | SQLAlchemy + Alembic |
| Dashboard | Streamlit + Plotly |

## Estructura del proyecto

```
etlweatherstack/
├── scripts/
│   ├── extractor.py        # Extrae datos de la API
│   ├── transformador.py    # Limpieza y transformación
│   ├── extractor_db.py     # Carga datos en PostgreSQL
│   ├── database.py         # Configuración SQLAlchemy
│   ├── models.py           # Modelos ORM
│   └── consultas.py        # Queries de análisis
├── dashboard_app.py        # Dashboard básico
├── dashboard_advanced.py   # Dashboard con 4 pestañas
├── dashboard_interactive.py # Dashboard interactivo completo ← principal
├── alembic/                # Migraciones de base de datos
├── .streamlit/
│   └── config.toml         # Configuración tema/puerto
└── requirements.txt
```

## Dashboards disponibles

| Archivo | Descripción |
|---|---|
| `dashboard_app.py` | Vista básica con 4 gráficos |
| `dashboard_advanced.py` | 4 pestañas: General, Histórico, Análisis, Métricas ETL |
| `dashboard_interactive.py` | Filtros completos, KPIs, descarga CSV |

## Ejecución local

```bash
# 1. Clonar y crear entorno
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Configurar credenciales
cp .env.example .env           # edita con tus credenciales
cp .streamlit/secrets.toml.example .streamlit/secrets.toml

# 3. Crear tablas e insertar datos
python scripts/test_db.py
python scripts/demo_data.py
python scripts/transformador.py
python scripts/extractor_db.py

# 4. Lanzar dashboard
streamlit run dashboard_interactive.py
```

## Variables de entorno requeridas

```toml
# .streamlit/secrets.toml (local) o Streamlit Cloud Secrets
DB_HOST     = "..."
DB_PORT     = "5432"
DB_USER     = "..."
DB_PASSWORD = "..."
DB_NAME     = "..."
```

## Despliegue en Streamlit Cloud

1. Fork/push este repositorio a GitHub
2. En [share.streamlit.io](https://share.streamlit.io) → **New app**
3. Selecciona el repositorio y `dashboard_interactive.py` como archivo principal
4. En **Advanced settings → Secrets**, pega el contenido de tu `secrets.toml`
5. Deploy
