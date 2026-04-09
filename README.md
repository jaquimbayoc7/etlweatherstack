# ETL Weatherstack — Pipeline de clima con Dashboard y Análisis Estadístico

Pipeline ETL completa que extrae datos climáticos de la API de Weatherstack para 5 ciudades colombianas (Bogotá, Medellín, Cali, Barranquilla, Cartagena), los transforma, almacena en PostgreSQL y los visualiza mediante dashboards interactivos y análisis de regresión.

## Tecnologías

| Capa | Herramienta |
|---|---|
| Extracción | Weatherstack API |
| Transformación | Python / Pandas |
| Almacenamiento | PostgreSQL (Supabase) |
| ORM / Migraciones | SQLAlchemy + Alembic |
| Dashboard | Streamlit + Plotly |
| Análisis estadístico | Scikit-learn, Statsmodels, SciPy |
| Notebooks | Jupyter Notebook |
| Visualización | Matplotlib, Seaborn, Plotly |

## Arquitectura del proyecto

```
Weatherstack API
      ↓
extractor.py → datos crudos (CSV/JSON)
      ↓
transformador.py → datos limpios y enriquecidos
      ↓
extractor_db.py → PostgreSQL (SQLAlchemy ORM)
      ↓
Dashboards (Streamlit + Plotly) ← visualización interactiva
      ↓
consultas.py / visualizador.py ← análisis de datos
      ↓
regresion_clima.ipynb ← modelado estadístico
```

## Estructura del proyecto

```
etlweatherstack/
├── scripts/
│   ├── extractor.py         # Extrae datos de la API Weatherstack
│   ├── transformador.py     # Limpieza, normalización y enriquecimiento
│   ├── extractor_db.py      # Carga datos transformados en PostgreSQL
│   ├── database.py          # Configuración SQLAlchemy y conexión DB
│   ├── models.py            # Modelos ORM (Ciudad, RegistroClima, MetricasETL)
│   ├── consultas.py         # Queries de análisis (stats, correlaciones)
│   ├── visualizador.py      # Generación de gráficos con Matplotlib
│   ├── demo_data.py         # Genera 1000 registros sintéticos de prueba
│   └── test_db.py           # Validación de conexión y creación de tablas
├── notebooks/
│   └── regresion_clima.ipynb # Regresión lineal/polinomial sobre datos climáticos
├── dashboard_app.py          # Dashboard básico (4 gráficos)
├── dashboard_advanced.py     # Dashboard avanzado (4 pestañas)
├── dashboard_interactive.py  # Dashboard interactivo completo ← principal
├── data/
│   ├── clima.csv             # Datos crudos extraídos
│   ├── clima_raw.json        # Datos crudos en JSON
│   ├── clima_transformado.csv # Datos limpios y enriquecidos
│   └── graficas/             # Gráficos generados
├── alembic/                  # Migraciones de base de datos
├── Steps/                    # Tutoriales HTML paso a paso
│   ├── etl-weatherstack-tutorial.html
│   ├── postgres-etl-database.html
│   ├── dashboard-streamlit-tutorial.html
│   ├── docker-containerization-tutorial.html
│   ├── jupyter-regression-tutorial.html
│   └── streamlit-cloud-supabase-deploy.html
├── .streamlit/
│   └── config.toml           # Configuración tema/puerto
├── alembic.ini
└── requirements.txt
```

## Modelos de base de datos

| Modelo | Descripción |
|---|---|
| `Ciudad` | Ciudades monitoreadas (nombre, país, coordenadas, estado activo) |
| `RegistroClima` | Registros climáticos (temperatura, humedad, viento, sensación térmica) con FK a Ciudad |
| `MetricasETL` | Métricas de ejecución del pipeline (registros procesados, tiempo, estado) |

## Scripts del pipeline

| Script | Fase | Descripción |
|---|---|---|
| `extractor.py` | Extract | Llama a la API Weatherstack y guarda datos crudos en CSV/JSON |
| `transformador.py` | Transform | Limpia duplicados, normaliza unidades, añade campos calculados (sensación térmica, categorías de viento) |
| `extractor_db.py` | Load | Lee datos transformados e inserta en PostgreSQL con tracking de métricas |
| `consultas.py` | Análisis | Estadísticas por ciudad, rankings de humedad, correlaciones |
| `visualizador.py` | Visualización | Genera gráficas estáticas con Matplotlib |
| `demo_data.py` | Testing | Genera 1000 registros sintéticos simulando 8 días de lecturas cada 30 min |
| `test_db.py` | Setup | Valida conexión a PostgreSQL y crea todas las tablas |

## Dashboards disponibles

| Archivo | Descripción |
|---|---|
| `dashboard_app.py` | Vista básica con 4 gráficos (temperatura, humedad, viento, presión) |
| `dashboard_advanced.py` | 4 pestañas: General, Histórico, Análisis, Métricas ETL |
| `dashboard_interactive.py` | **Principal** — Filtros por ciudad/fecha, KPIs en tiempo real, descarga CSV, CSS personalizado |

## Notebook de análisis

**`notebooks/regresion_clima.ipynb`** — Análisis de regresión sobre los datos climáticos:
- Modelos de regresión lineal y polinomial
- Cálculo de R², residuos y métricas de error
- Pronóstico de variables climáticas
- Visualizaciones estadísticas con Seaborn y Matplotlib

## Tutoriales incluidos

La carpeta `Steps/` contiene guías HTML paso a paso:

| Tutorial | Contenido |
|---|---|
| `etl-weatherstack-tutorial.html` | Construcción del pipeline ETL completo |
| `postgres-etl-database.html` | Configuración de PostgreSQL y carga de datos |
| `dashboard-streamlit-tutorial.html` | Creación de dashboards con Streamlit |
| `jupyter-regression-tutorial.html` | Análisis de regresión con Jupyter |
| `docker-containerization-tutorial.html` | Contenerización con Docker |
| `streamlit-cloud-supabase-deploy.html` | Despliegue en Streamlit Cloud + Supabase |

## Ejecución local

```bash
# 1. Clonar y crear entorno
git clone https://github.com/jaquimbayoc7/etlweatherstack.git
cd etlweatherstack
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Configurar credenciales
cp .env.example .env           # edita con tus credenciales
cp .streamlit/secrets.toml.example .streamlit/secrets.toml

# 3. Crear tablas e insertar datos
python scripts/test_db.py       # verifica conexión y crea tablas
python scripts/demo_data.py     # genera datos de prueba
python scripts/transformador.py # transforma los datos
python scripts/extractor_db.py  # carga en PostgreSQL

# 4. Lanzar dashboard
streamlit run dashboard_interactive.py

# 5. (Opcional) Abrir notebook de regresión
jupyter notebook notebooks/regresion_clima.ipynb
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

## Dependencias principales

```
requests        — Llamadas a la API
pandas          — Manipulación de datos
SQLAlchemy      — ORM para PostgreSQL
alembic         — Migraciones de base de datos
streamlit       — Dashboards interactivos
plotly          — Gráficos interactivos
matplotlib      — Gráficos estáticos
seaborn         — Visualización estadística
scikit-learn    — Modelos de regresión
statsmodels     — Análisis estadístico avanzado
scipy           — Herramientas científicas
jupyter         — Notebooks interactivos
psycopg2-binary — Driver PostgreSQL
```

## Despliegue en Streamlit Cloud

1. Fork/push este repositorio a GitHub
2. En [share.streamlit.io](https://share.streamlit.io) → **New app**
3. Selecciona el repositorio y `dashboard_interactive.py` como archivo principal
4. En **Advanced settings → Secrets**, pega el contenido de tu `secrets.toml`
5. Deploy
