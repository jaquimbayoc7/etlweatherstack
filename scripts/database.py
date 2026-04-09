#!/usr/bin/env python3
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
import logging

load_dotenv(override=True)

logger = logging.getLogger(__name__)

# Lee credenciales: primero intenta st.secrets (solo en Streamlit Cloud),
# si no está disponible usa variables de entorno (.env local)
def _is_running_in_streamlit():
    """Detecta si estamos dentro de un proceso Streamlit real."""
    try:
        from streamlit.runtime import get_instance
        return get_instance() is not None
    except Exception:
        return False

def _get_db_config():
    # Intento 1: st.secrets (solo cuando realmente corre dentro de Streamlit)
    if _is_running_in_streamlit():
        try:
            import streamlit as st
            host = st.secrets.get("DB_HOST", "")
            if host:
                return {
                    "host":     host,
                    "port":     st.secrets.get("DB_PORT", "5432"),
                    "user":     st.secrets.get("DB_USER", "postgres"),
                    "password": st.secrets.get("DB_PASSWORD", ""),
                    "dbname":   st.secrets.get("DB_NAME", "postgres"),
                }
        except Exception:
            pass

    # Intento 2: variables de entorno / .env (desarrollo local y scripts CLI)
    host = os.getenv("DB_HOST", "localhost")
    return {
        "host":     host,
        "port":     os.getenv("DB_PORT", "5432"),
        "user":     os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", ""),
        "dbname":   os.getenv("DB_NAME", "weatherstack_etl"),
    }

_cfg = _get_db_config()
DB_HOST     = _cfg["host"]
DB_PORT     = _cfg["port"]
DB_USER     = _cfg["user"]
DB_PASSWORD = _cfg["password"]
DB_NAME     = _cfg["dbname"]

# URL de conexión — quote_plus codifica caracteres especiales en la contraseña (@, #, etc.)
# sslmode=require solo para hosts remotos (Supabase); prefer para localhost
_sslmode = "require" if DB_HOST not in ("localhost", "127.0.0.1") else "prefer"
DATABASE_URL = (
    f"postgresql://{quote_plus(DB_USER)}:{quote_plus(DB_PASSWORD)}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={_sslmode}"
)

# Fuerza IPv4 para evitar problemas de conectividad IPv6 en WSL
_connect_args = {"options": "-c timezone=UTC"}
if DB_HOST not in ("localhost", "127.0.0.1"):
    _connect_args["host"] = DB_HOST  # psycopg2 resuelve IPv4 primero

# Motor SQLAlchemy
engine = create_engine(DATABASE_URL, echo=False, connect_args=_connect_args)

# Base para modelos ORM
Base = declarative_base()

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """Obtiene una sesión de base de datos"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def test_connection():
    """Prueba la conexión a la base de datos"""
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            logger.info("✅ Conexión exitosa a PostgreSQL")
            return True
    except Exception as e:
        logger.error(f"❌ Error conectando a PostgreSQL: {str(e)}")
        return False


def create_all_tables():
    """Crea todas las tablas definidas en los modelos"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Tablas creadas exitosamente")
    except Exception as e:
        logger.error(f"❌ Error creando tablas: {str(e)}")
        raise
