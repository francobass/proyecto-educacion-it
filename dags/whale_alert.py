from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
import requests
import boto3
import datetime
import os
import io
import logging
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine, text

# Configuración del logging
log = logging.getLogger(__name__)

# URL de la página de Whale Alert a scrapear
URL_WHALE_ALERT = "https://whale-alert.io/whales.html"

# MinIO
BUCKET = "datalake"
SOURCE = "whale_alert"
DATASET = "whale_alerts_limits"
FILENAME = "whale_alerts_limits.html"

# PostgreSQL
TARGET_SCHEMA = "staging"
TABLE_NAME = "whale_alerts_limits"



def extract(**context) -> str:
    log.info("Solicitando datos a %s", URL_WHALE_ALERT)
    response = requests.get(URL_WHALE_ALERT)

    log.info("Resultado de la request de la API: %s", response.status_code)

    buffer = io.BytesIO(response.content)

    extraction_ts = context["logical_date"]
    key = (
        f"source={SOURCE}/dataset={DATASET}/year={extraction_ts:%Y}/month={extraction_ts:%m}/"
        f"{extraction_ts:%Y%m%dT%H%M%S}_{FILENAME}"
    )
    log.info("Filename path en MinIO: %s", key)

    # Carga del archivo en el bucket landing de MinIO
    minio = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
    )

    # Verificar si el bucket existe, si no, crearlo
    try:
        minio.head_bucket(Bucket=BUCKET)
        log.info("Bucket '%s' ya existe", BUCKET)
    except minio.exceptions.ClientError:
        log.info("Bucket '%s' no existe, creandolo", BUCKET)
        minio.create_bucket(Bucket=BUCKET)

    # Carga del archivo en MinIO
    minio.upload_fileobj(buffer, BUCKET, key)
    log.info("Archivo subido a MinIO: bucket=%s key=%s", BUCKET, key)

    return key

def transform_and_load(**context):
    # Recuperar la key (file path en MinIO) del archivo desde XCom
    key = context["ti"].xcom_pull(task_ids="extract")
    log.info("Key recibida via XCom: %s", key)

    # Leer el archivo desde MinIO
    minio = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
    )

    # Descargar el archivo desde MinIO 
    obj = minio.get_object(Bucket=BUCKET, Key=key)
    # Leer el contenido del archivo y decodificar html a UTF-8
    html_content = obj["Body"].read().decode("utf-8")
    log.info("Archivo leido desde MinIO: bucket=%s key=%s (%d bytes)", BUCKET, key, len(html_content))

    # Parsear el contenido HTML usando BeautifulSoup
    soup = BeautifulSoup(html_content, "html.parser")

    table = soup.find("table")
    tbody = table.find("tbody")

    rows =tbody.find_all("tr")
    log.info("Filas encontradas en la tabla: %d", len(rows))

    data = []
    for row in rows:
        th = row.find("th", {"scope": "row"})
        img = th.find("img")
        coin_name = img["alt"].strip() if img else th.get_text(strip=True)
        row_data = row.find_all("td")

        # Crear un diccionario con los datos parseados (cada fila de la tabla)
        json_data = {
            "coin_name": coin_name,
            "known": row_data[0].text.strip(),
            "unknown": row_data[1].text.strip(),
        }

        # Agregar la fila parseada a la lista de datos
        data.append(json_data)

    log.info("Filas parseadas: %d", len(data))

    # Crear un DataFrame de pandas a partir de los datos parseados
    df = pd.DataFrame(data)
    # Agregar la columna de timestamp de extracción al DataFrame
    df["extraction_ts"] = context["logical_date"]

    # Crear la conexión a PostgreSQL usando SQLAlchemy
    engine = create_engine(
        f"postgresql://{os.getenv('POSTGRES_USER', 'admin')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'admin123')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres_db')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'local_db')}"
    )

    # Carga de los datos en la tabla de PostgreSQL
    log.info("Cargando %d filas en %s.%s", len(df), TARGET_SCHEMA, TABLE_NAME)
    df.to_sql(TABLE_NAME, engine, schema=TARGET_SCHEMA, if_exists="append", index=False)
    log.info("Carga en PostgreSQL completada: %s.%s", TARGET_SCHEMA, TABLE_NAME)
    engine.dispose()

# Definición del DAG de Airflow
with DAG(
    dag_id="whale_alert",
    start_date=datetime.datetime(2026, 9, 1),
    end_date=datetime.datetime(2027, 1, 1),
    catchup=False,
    schedule="@daily",
):

    start_task = EmptyOperator(task_id="start_task")

    extract = PythonOperator(task_id="extract", python_callable=extract)

    transform_and_load = PythonOperator(task_id="transform_and_load", python_callable=transform_and_load)

    end_task = EmptyOperator(task_id="end_task")

    start_task >> extract >> transform_and_load >> end_task

