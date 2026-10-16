"""DAG AMN seed 2/3 - Transformación: descomprime los ZIP de rawdata y guarda los CSV en staging.

Lee de MinIO (bucket rawdata) y escribe transformed_<dataset>.csv en el bucket staging.
El CSV se copia en streaming (por partes), sin cargarlo entero en memoria: Purchases tiene 2.3M filas.
Paso anterior: aux_amn_1_data_ingestion_dag. Siguiente: aux_amn_3_database_loader_dag.
Lo dispara aux_amn_0_master_ingesta.
"""

import logging
import zipfile
from datetime import datetime
from io import BytesIO

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from utils.connections import crear_bucket_si_no_existe, get_minio_client

log = logging.getLogger(__name__)

RAW_BUCKET = "rawdata"
STAGING_BUCKET = "staging"

# ZIP de origen -> nombre del dataset
TARGET_FILES = {
    "PurchasesFINAL12312016csv.zip": "Purchases",
    "VendorInvoices12312016csv.zip": "VendorInvoices",
    "2017PurchasePricesDeccsv.zip": "purchasePrices",
    "BegInvFINAL12312016csv.zip": "BegInv",
    "EndInvFINAL12312016csv.zip": "EndInv",
}


def transform_and_save_to_datalake(**context):
    """Extrae el CSV de cada ZIP de rawdata y lo sube al bucket staging."""
    s3_client = get_minio_client()
    # Versionado: si se vuelve a correr, MinIO guarda el CSV nuevo como otra versión
    crear_bucket_si_no_existe(s3_client, STAGING_BUCKET, versionado=True)

    objects = s3_client.list_objects_v2(Bucket=RAW_BUCKET)
    files = {obj["Key"] for obj in objects.get("Contents", [])}
    log.info("Archivos encontrados en %s: %s", RAW_BUCKET, files)

    for file, dataset_name in TARGET_FILES.items():
        if file not in files:
            log.warning("Archivo %s no encontrado en el bucket %s", file, RAW_BUCKET)
            continue

        log.info("Procesando %s", file)
        # El ZIP (comprimido) sí va entero a memoria: zipfile necesita saltar dentro del archivo
        # (el índice está al final) y el Body de MinIO sólo se lee de corrido. El CSV, no.
        zip_content = BytesIO(
            s3_client.get_object(Bucket=RAW_BUCKET, Key=file)["Body"].read()
        )

        with zipfile.ZipFile(zip_content) as zip_ref:
            csv_name = zip_ref.namelist()[0]  # cada ZIP trae un solo CSV
            output_key = f"transformed_{dataset_name}.csv"
            with zip_ref.open(csv_name) as extracted_file:
                # upload_fileobj sube el archivo por partes mientras se descomprime
                s3_client.upload_fileobj(extracted_file, STAGING_BUCKET, output_key)
        log.info("%s -> %s/%s", csv_name, STAGING_BUCKET, output_key)

        # Dejar la ruta en XCom (se puede ver en la UI de Airflow)
        context["ti"].xcom_push(
            key=f"{dataset_name}_csv_path", value=f"{STAGING_BUCKET}/{output_key}"
        )


with DAG(
    "aux_amn_2_data_transformation_dag",
    description="Descomprime los ZIP de rawdata en CSV dentro de staging (MinIO)",
    schedule=None,  # sólo se ejecuta a mano
    start_date=datetime(2023, 1, 1),
    catchup=False,
):
    PythonOperator(
        task_id="transform_and_save_to_datalake",
        python_callable=transform_and_save_to_datalake,
    )
