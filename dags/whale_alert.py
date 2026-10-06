"""DAG Whale Alert escrito de la forma "clásica" (with DAG + PythonOperator).

Flujo:
    start_task -> extract -> transform_and_load -> end_task

    extract            -> descarga el HTML de whale-alert.io y lo guarda en MinIO (capa raw)
    transform_and_load -> lee el HTML de MinIO, arma una tabla y la carga en PostgreSQL

Las tareas se pasan datos por XCom: lo que devuelve extract se lee con xcom_pull.
"""

import datetime
import io
import logging

import pandas as pd
import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from bs4 import BeautifulSoup
from sqlalchemy import text

from utils.connections import (
    crear_bucket_si_no_existe,
    get_minio_client,
    get_postgres_engine,
)
from utils.layer import ejecutar_ddl

log = logging.getLogger(__name__)

# URL de la página de Whale Alert a scrapear
URL_WHALE_ALERT = "https://whale-alert.io/whales.html"

# MinIO
BUCKET = "datalake"
SOURCE = "whale_alert"
DATASET = "whale_alerts_limits"
FILENAME = "whale_alerts_limits.html"

# PostgreSQL (la tabla se crea con ddl/postgres_dw/15_whale_alert.sql)
TARGET_SCHEMA = "bronze"
TABLE_NAME = "whale_alerts_limits"


def extract(**context) -> str:
    """Descarga el HTML, lo sube a MinIO y devuelve la key (ruta) del archivo."""
    log.info("Solicitando datos a %s", URL_WHALE_ALERT)
    response = requests.get(URL_WHALE_ALERT, timeout=30)
    # Si la página responde con error (4xx/5xx) la tarea falla y no guardamos basura
    response.raise_for_status()

    # Ruta particionada por fecha: source=.../dataset=.../year=2026/month=09/<fecha>_archivo.html
    extraction_ts = context["logical_date"]
    key = (
        f"source={SOURCE}/dataset={DATASET}/year={extraction_ts:%Y}/month={extraction_ts:%m}/"
        f"{extraction_ts:%Y%m%dT%H%M%S}_{FILENAME}"
    )

    minio = get_minio_client()
    crear_bucket_si_no_existe(minio, BUCKET)
    minio.upload_fileobj(io.BytesIO(response.content), BUCKET, key)
    log.info("Archivo subido a MinIO: bucket=%s key=%s", BUCKET, key)

    # Lo que devuelve la función queda guardado en XCom
    return key


def transform_and_load(**context) -> None:
    """Lee el HTML desde MinIO, extrae la tabla y la carga en PostgreSQL."""
    # Recuperar la key que devolvió la tarea extract
    key = context["ti"].xcom_pull(task_ids="extract")
    extraction_ts = context["logical_date"]

    minio = get_minio_client()
    obj = minio.get_object(Bucket=BUCKET, Key=key)
    html_content = obj["Body"].read().decode("utf-8")
    log.info("Archivo leído desde MinIO: %s (%d bytes)", key, len(html_content))

    # Parsear el HTML: cada fila <tr> es una moneda
    soup = BeautifulSoup(html_content, "html.parser")
    rows = soup.find("table").find("tbody").find_all("tr")

    data = []
    for row in rows:
        # El nombre de la moneda está en el alt de la imagen (o en el texto del <th>)
        th = row.find("th", {"scope": "row"})
        img = th.find("img")
        coin_name = img["alt"].strip() if img else th.get_text(strip=True)
        cells = row.find_all("td")
        data.append(
            {
                "coin_name": coin_name,
                "known": cells[0].text.strip(),
                "unknown": cells[1].text.strip(),
            }
        )
    log.info("Filas parseadas: %d", len(data))

    df = pd.DataFrame(data)
    df["extraction_ts"] = extraction_ts

    # Crea la tabla si todavía no existe (por ejemplo, si la base se creó con una versión anterior)
    ejecutar_ddl("15_whale_alert.sql")

    engine = get_postgres_engine()
    # Carga idempotente: si la tarea se reintenta, primero borramos lo cargado para esa fecha
    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {TARGET_SCHEMA}.{TABLE_NAME} WHERE extraction_ts = :ts"),
            {"ts": extraction_ts},
        )
        df.to_sql(
            TABLE_NAME, conn, schema=TARGET_SCHEMA, if_exists="append", index=False
        )
    engine.dispose()
    log.info("Cargadas %d filas en %s.%s", len(df), TARGET_SCHEMA, TABLE_NAME)


with DAG(
    dag_id="whale_alert",
    start_date=datetime.datetime(2026, 9, 1),
    end_date=datetime.datetime(2027, 1, 1),
    catchup=False,
    schedule="@daily",
):
    start_task = EmptyOperator(task_id="start_task")
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_and_load_task = PythonOperator(
        task_id="transform_and_load", python_callable=transform_and_load
    )
    end_task = EmptyOperator(task_id="end_task")

    start_task >> extract_task >> transform_and_load_task >> end_task
