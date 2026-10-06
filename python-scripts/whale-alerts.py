"""Scraping de Whale Alert y guardado local en CSV, JSON y Parquet.

Script de práctica (sin Airflow ni MinIO). Ejecutar desde la raíz del repo:
    python whale-alerts.py
Los archivos quedan en la carpeta data/.
"""

import os
from datetime import date

import pandas as pd
import requests
from bs4 import BeautifulSoup

url_whale_alert = "https://whale-alert.io/whales.html"

# 1. Extracción: descargar la página
response = requests.get(url_whale_alert, timeout=30)
response.raise_for_status()  # corta el script si la página devuelve un error
print(response)

# 2. Transformación: buscar la tabla y recorrer sus filas
soup = BeautifulSoup(response.content, "html.parser")
rows = soup.find("table").find("tbody").find_all("tr")

data = []
for row in rows:
    # El nombre de la moneda está en el alt de la imagen (o en el texto del <th>)
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

# 3. Carga: guardar en distintos formatos para compararlos
os.makedirs("data", exist_ok=True)
df.to_csv("data/whale_alerts.csv", index=False)
df.to_json("data/whale_alerts.json", orient="records", lines=True)
df.to_parquet("data/whale_alerts.parquet", index=False)

print(df)
