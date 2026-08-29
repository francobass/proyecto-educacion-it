import requests
from bs4 import BeautifulSoup
import pandas as pd

url_whale_alert = "https://whale-alert.io/whales.html"

response = requests.get(url_whale_alert)

print(response)
#print(response.text)


soup = BeautifulSoup(response.content, "html.parser")

with open("data/whale_alerts.html", "w", encoding="utf-8") as html_file:
    html_file.write(str(soup))

print(soup)

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

df = pd.DataFrame(data)
df['date_extraction'] = "2026-08-15"

df.to_csv("data/whale_alerts.csv", index=False)
df.to_json("data/whale_alerts.json", orient="records", lines=True)
df.to_parquet("data/whale_alerts.parquet", index=False)

print(df)


