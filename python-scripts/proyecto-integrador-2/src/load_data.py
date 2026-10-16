"""Carga los CSV de la carpeta data/ en las tablas creadas por create_tables.py."""

from pathlib import Path

import pandas as pd
from db_config import DB_URL
from sqlalchemy import create_engine, text

# (archivo CSV, tabla destino, opciones extra para pd.read_csv)
# - sep=";" y decimal="," porque esos archivos vienen con formato "Excel en español"
CSV_A_TABLAS = [
    ("Calendario.csv", "calendario", {}),
    ("CanalDeVenta.csv", "canaldeventa", {}),
    ("Clientes.csv", "clientes", {"sep": ";", "decimal": ","}),
    ("Productos.csv", "productos", {}),
    ("Sucursales.csv", "sucursales", {"sep": ";", "decimal": ","}),
    ("Venta.csv", "venta", {"dtype": {"Cantidad": "Int64"}}),  # Int64 admite nulos
]


def leer_csv(path: Path, opciones: dict) -> pd.DataFrame:
    """Lee un CSV y normaliza los nombres de columna para que coincidan con el DDL."""
    # utf-8-sig elimina el BOM que traen algunos archivos exportados desde Excel
    df = pd.read_csv(path, encoding="utf-8-sig", **opciones)
    # Postgres guarda en minúscula los nombres sin comillas (ej. "Precio " -> "precio")
    df.columns = [col.strip().lower() for col in df.columns]
    return df


def load_data():
    """Vacía las tablas y vuelve a cargar todos los CSV.

    Usamos TRUNCATE + append (y no if_exists="replace") para respetar las tablas
    del DDL: replace las borra y las recrea perdiendo PKs y tipos de datos.
    """
    data_dir = Path(__file__).parent.parent / "data"
    engine = create_engine(DB_URL)

    # engine.begin() abre una transacción: si algo falla, no queda nada a medio cargar
    with engine.begin() as conn:
        # Todas en un solo TRUNCATE: si se agregan FKs (ej. venta -> clientes), Postgres
        # exige vaciar en el mismo comando la tabla referenciada y la que la referencia.
        tablas = ", ".join(tabla for _, tabla, _ in CSV_A_TABLAS)
        conn.execute(text(f"TRUNCATE {tablas}"))

        for archivo, tabla, opciones in CSV_A_TABLAS:
            df = leer_csv(data_dir / archivo, opciones)
            df.to_sql(tabla, conn, if_exists="append", index=False)
            print(f"{archivo}: {len(df)} filas cargadas en {tabla}")

    engine.dispose()
    print("Todos los datos cargados")
