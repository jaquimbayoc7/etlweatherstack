#!/usr/bin/env python3
import sys
sys.path.insert(0, '.')

from scripts.database import test_connection, engine, create_all_tables
import scripts.models  # noqa: F401 — registra los modelos en Base.metadata
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("PRUEBA DE CONEXIÓN A POSTGRESQL")
    print("=" * 50)

    if test_connection():
        print("✅ Conexión exitosa a la base de datos")
        print(f"   Base de datos : {engine.url.database}")
        print(f"   Host          : {engine.url.host}")
        print(f"   Puerto        : {engine.url.port}")

        print("\n📋 Creando tablas...")
        create_all_tables()
        print("✅ Tablas listas")
    else:
        print("❌ No se pudo conectar a la base de datos")
        print("\nVerifica:")
        print("  - PostgreSQL está corriendo  (sudo service postgresql start)")
        print("  - Variables en .env son correctas")
        print("  - La base de datos existe    (CREATE DATABASE weatherstack_etl;)")
        sys.exit(1)

    print("=" * 50 + "\n")
