"""Proyecto integrador 1: ETL de Whale Alert con MinIO, sin Airflow.

1. Extrae el HTML de whale-alert.io y lo sube al bucket "landing".
2. Transforma la tabla HTML en un DataFrame.
3. Guarda el resultado como Parquet en el bucket "bronze".

Requiere MinIO levantado (docker compose up -d). Ejecutar desde la raíz del repo:
    python python-scripts/proyecto-integrador-1/whale-alerts-borrador-script.py
"""

import os
from datetime import date

import boto3
import pandas as pd
import requests
from bs4 import BeautifulSoup


def get_minio_client():
    """Cliente S3 para el MinIO local (desde la PC es localhost:9000)."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
    )


def subir_a_minio(minio, file_path, bucket, key):
    """Sube un archivo local a MinIO, creando el bucket si no existe."""
    try:
        minio.head_bucket(Bucket=bucket)
    except minio.exceptions.ClientError:
        minio.create_bucket(Bucket=bucket)
    minio.upload_file(file_path, bucket, key)


os.makedirs("data", exist_ok=True)
minio = get_minio_client()

# Extracción de los datos
url_whale_alert = "https://whale-alert.io/whales.html"
response = requests.get(url_whale_alert, timeout=30)
response.raise_for_status()

# Guardamos el HTML crudo (response.text, no str(response) que sólo dice "<Response [200]>")
with open("data/whale_alerts.html", "w", encoding="utf-8") as html_file:
    html_file.write(response.text)

subir_a_minio(minio, "data/whale_alerts.html", "landing", "whale_alerts.html")

# Transformación de los datos
soup = BeautifulSoup(response.content, "html.parser")
rows = soup.find("table").find("tbody").find_all("tr")

data = []
for row in rows:
    th = row.find("th", {"scope": "row"})
    img = th.find("img")
    coin_name = img["alt"].strip() if img else th.get_text(strip=True)
    row_data = row.find_all("td")

    data.append(
        {
            "coin_name": coin_name,
            "known": row_data[0].text.strip(),
            "unknown": row_data[1].text.strip(),
        }
    )

df = pd.DataFrame(data)
df["date_extraction"] = date.today().isoformat()

# Carga de los datos
df.to_parquet("data/whale_alerts.parquet", index=False)
subir_a_minio(minio, "data/whale_alerts.parquet", "bronze", "whale_alerts.parquet")

print(df)
