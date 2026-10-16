"""DAG AMN seed 1/3 - Ingesta: descarga los ZIP de PwC y los guarda en MinIO (bucket rawdata).

Flujo: check_connection -> ingest_data
Siguiente paso: aux_amn_2_data_transformation_dag. Lo dispara aux_amn_0_master_ingesta.
"""

import io
import logging
from datetime import datetime

import requests
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from utils.connections import crear_bucket_si_no_existe, get_minio_client

log = logging.getLogger(__name__)

RAW_BUCKET = "rawdata"

# Archivos a descargar (casos de estudio de PwC, en formato ZIP con un CSV adentro).
# El de Sales no se descarga: no se usa en el curso porque tiene demasiado volumen.
URLS = [
    "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/PurchasesFINAL12312016csv.zip",
    "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/BegInvFINAL12312016csv.zip",
    "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/2017PurchasePricesDeccsv.zip",
    "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/VendorInvoices12312016csv.zip",
    "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/EndInvFINAL12312016csv.zip",
]


def check_minio_connection():
    """Verifica que MinIO responde. Si no, lanza una excepción y la tarea falla."""
    response = get_minio_client().list_buckets()
    log.info(
        "Conexión a MinIO exitosa. Buckets: %s",
        [b["Name"] for b in response["Buckets"]],
    )


def ingest_data_to_datalake():
    """Descarga cada ZIP y lo sube tal cual al bucket rawdata."""
    s3_client = get_minio_client()
    crear_bucket_si_no_existe(s3_client, RAW_BUCKET, versionado=True)

    for url in URLS:
        file_name = url.split("/")[-1]
        log.info("Descargando %s", file_name)

        response = requests.get(url, timeout=300)
        response.raise_for_status()  # si la descarga falla, la tarea falla

        s3_client.upload_fileobj(io.BytesIO(response.content), RAW_BUCKET, file_name)
        log.info("Archivo %s subido a %s", file_name, RAW_BUCKET)


with DAG(
    "aux_amn_1_data_ingestion_dag",
    description="Descarga archivos de PwC y los sube al data lake (MinIO)",
    schedule=None,  # sólo se ejecuta a mano
    start_date=datetime(2025, 1, 1),
    catchup=False,
):
    check_connection_task = PythonOperator(
        task_id="check_connection",
        python_callable=check_minio_connection,
    )

    ingest_task = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data_to_datalake,
    )

    check_connection_task >> ingest_task
