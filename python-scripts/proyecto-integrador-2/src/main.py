"""Proyecto integrador 2: crea las tablas y carga los CSV en PostgreSQL.

Requiere el postgres_db levantado (docker compose up -d). Ejecutar desde esta carpeta:
    cd python-scripts/proyecto-integrador-2/src
    python main.py
"""

from create_tables import create_empty_tables
from load_data import load_data

if __name__ == "__main__":
    create_empty_tables()
    load_data()
