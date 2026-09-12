import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import boto3



# Extracción de los datos
url_whale_alert = "https://whale-alert.io/whales.html"

response = requests.get(url_whale_alert)

with open("data/whale_alerts.html", "w", encoding="utf-8") as html_file:
    html_file.write(str(response))

# Carga del archivo en el bucket landing de MinIO
minio = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password"),
    region_name=os.getenv("MINIO_REGION", "us-east-1"),
)

bucket = "landing"
file_path = "data/whale_alerts.html"

try:
    minio.head_bucket(Bucket=bucket)
except minio.exceptions.ClientError:
    minio.create_bucket(Bucket=bucket)

minio.upload_file(file_path, bucket, "whale_alerts.html")


# Trasnformación de los datos
soup = BeautifulSoup(response.content, "html.parser")

table = soup.find("table")
tbody = table.find("tbody")

rows =tbody.find_all("tr")

data = []
for row in rows:
    th = row.find("th", {"scope": "row"})
    img = th.find("img")
    coin_name = img["alt"].strip() if img else th.get_text(strip=True)
    row_data = row.find_all("td")

    json_data = {
        "coin_name": coin_name,
        "known": row_data[0].text.strip(),
        "unknown": row_data[1].text.strip(),
    }

    data.append(json_data)

print(data)

df = pd.DataFrame(data)
df['date_extraction'] = "2026-08-15"


# Carga de los datos
df.to_parquet("data/whale_alerts.parquet", index=False)

# Carga del archivo en el bucket landing de MinIO
minio = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password"),
    region_name=os.getenv("MINIO_REGION", "us-east-1"),
)

bucket = "bronze"
file_path = "data/whale_alerts.parquet"

try:
    minio.head_bucket(Bucket=bucket)
except minio.exceptions.ClientError:
    minio.create_bucket(Bucket=bucket)

minio.upload_file(file_path, bucket, "whale_alerts.parquet")

print(df)