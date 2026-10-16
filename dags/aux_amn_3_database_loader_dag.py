"""DAG AMN seed 3/3 - Carga: lleva los CSV del bucket staging a la base origen "amn".

La base amn vive en postgres_airflow y simula el sistema transaccional de la empresa
(conexión de Airflow "amn_origen"). Después de correr este DAG se activan los DAGs diarios
amn_purchases, amn_vendor_invoices, amn_purchase_prices, amn_beginv y amn_endinv,
que llevan los datos de amn al datalake (Parquet) y de ahí a bronze y silver.
Paso anterior: aux_amn_2_data_transformation_dag. Lo dispara aux_amn_0_master_ingesta.

Cada tabla se carga primero en <tabla>_nueva y recién al final se reemplaza la tabla real
(en una sola transacción). Así, si los DAGs diarios leen mientras corre el seed, ven la tabla
vieja completa o la nueva completa, nunca una a medio cargar.
"""

import logging
from datetime import datetime

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from sqlalchemy import text
from utils.connections import get_minio_client

log = logging.getLogger(__name__)

STAGING_BUCKET = "staging"
CHUNK_SIZE = 100_000  # filas por lote, para no cargar todo el archivo en memoria de una

# CSV en staging -> (tabla en amn, columnas de fecha, columna a indexar)
TARGET_FILES = {
    "transformed_Purchases.csv": (
        "purchases",
        ["podate", "receivingdate", "invoicedate", "paydate"],
        "podate",
    ),
    "transformed_VendorInvoices.csv": (
        "vendorinvoices",
        ["invoicedate", "podate", "paydate"],
        "invoicedate",
    ),
    "transformed_purchasePrices.csv": ("purchaseprices", [], None),
    "transformed_BegInv.csv": ("beginv", ["startdate"], "startdate"),
    "transformed_EndInv.csv": ("endinv", ["enddate"], "enddate"),
}


def load_to_database():
    """Lee cada CSV de staging por lotes y lo escribe en la base amn (reemplazo atómico)."""
    engine = PostgresHook(postgres_conn_id="amn_origen").get_sqlalchemy_engine()
    s3_client = get_minio_client()

    for file, (table, date_columns, index_column) in TARGET_FILES.items():
        log.info("Procesando %s -> %s", file, table)
        # El Body de MinIO es un stream: pandas lo va leyendo de a CHUNK_SIZE filas
        body = s3_client.get_object(Bucket=STAGING_BUCKET, Key=file)["Body"]

        tabla_nueva = f"{table}_nueva"
        for i, chunk in enumerate(pd.read_csv(body, chunksize=CHUNK_SIZE)):
            # Nombres de columna en minúscula y sin espacios (más cómodos en SQL)
            chunk.columns = [
                col.strip().lower().replace(" ", "_") for col in chunk.columns
            ]
            # Las fechas vienen como texto: las convertimos para que en Postgres sean TIMESTAMP
            for col in date_columns:
                chunk[col] = pd.to_datetime(chunk[col])

            # El primer lote recrea la tabla nueva; los siguientes se agregan al final.
            # Los tipos de columna los infiere pandas de ese primer lote (y de ahí los copia bronze)
            chunk.to_sql(
                tabla_nueva,
                engine,
                if_exists="replace" if i == 0 else "append",
                index=False,
            )
            log.info("Lote %d: %d filas cargadas en %s", i, len(chunk), tabla_nueva)

        # Reemplazo atómico: en una transacción se borra la tabla vieja y se renombra la nueva
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
            conn.execute(text(f"ALTER TABLE {tabla_nueva} RENAME TO {table}"))
            # Índice en la columna de fecha: los DAGs diarios filtran por ella
            if index_column:
                conn.execute(
                    text(
                        f"CREATE INDEX IF NOT EXISTS ix_{table}_{index_column} ON {table} ({index_column})"
                    )
                )
        log.info("Tabla %s reemplazada", table)

    engine.dispose()


with DAG(
    "aux_amn_3_database_loader_dag",
    description="Carga los CSV de staging (MinIO) en la base origen amn",
    schedule=None,  # sólo se ejecuta a mano
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,  # una carga a la vez: dos corridas pisarían la misma <tabla>_nueva
):
    PythonOperator(
        task_id="load_to_database",
        python_callable=load_to_database,
    )
