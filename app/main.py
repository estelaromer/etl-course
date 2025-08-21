import json
import os
import requests

import pandas as pd

from db import get_connection, load_sales_data


# CSV file
df = pd.read_csv("./data/sales.csv")
print(df.head())

# JSON file
with open("./data/users.json") as f:
    data = json.load(f)

for user in data:
    print(user["name"])

# PostgreSQL Database
conn = get_connection()

load_sales_data(conn)

query = "SELECT * FROM sales WHERE date >= '2025-01-01'"

df = pd.read_sql(query, conn)
print(df.head())

conn.close()

# API
api_key=os.environ.get("API_KEY", ""),

url = "http://api.marketstack.com/v2/eod/latest"
params = {
    "access_key": api_key,
    "symbols": "AAPL"
}

response = requests.get(url, params=params)
data = response.json()
print(data['data'])

