"""Crea las tablas vacías ejecutando los scripts SQL de queries/DDL (en orden)."""

from pathlib import Path

import psycopg2
from db_config import DB_CONFIG

# Orden de ejecución de los scripts DDL
SQL_SCRIPTS = [
    "01_create_calendario.sql",
    "02_create_canal_venta.sql",
    "03_create_clientes.sql",
    "04_create_productos.sql",
    "05_create_sucursales.sql",
    "06_create_venta.sql",
]


def create_empty_tables():
    """Ejecuta cada script SQL. Usan CREATE TABLE IF NOT EXISTS, así que se puede correr varias veces."""
    ddl_dir = Path(__file__).parent.parent / "queries" / "DDL"

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True  # cada sentencia se confirma sola

    with conn.cursor() as cursor:
        for script_name in SQL_SCRIPTS:
            print(f"Ejecutando: {script_name}")
            cursor.execute((ddl_dir / script_name).read_text(encoding="utf-8"))

    conn.close()
    print("Todas las tablas creadas")
