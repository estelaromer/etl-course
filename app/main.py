import json
import os
import requests

import pandas as pd

from db import get_connection, load_sales_data

# Extract

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

df_1 = pd.read_sql(query, conn)
print(df_1.head())

conn.close()

# API
# api_key=os.environ.get("API_KEY", ""),

# url = "http://api.marketstack.com/v2/eod/latest"
# params = {
#     "access_key": api_key,
#     "symbols": "AAPL"
# }

# response = requests.get(url, params=params)
# data = response.json()
# print(data['data'])

# Transform sales data

# Remove duplicate rows
df = df.drop_duplicates()

# Fill missing prices with 0
df["Price"] = df["Price"].fillna(0)

# Make all product names lowercase for consistency
df["Product"] = df["Product"].str.lower()

print('Sales Data transformed:')
print(df)

print(df['Date'].dtype)
print(df['Product'].dtype)
print(df['Price'].dtype)

# Convert string to datetime
df["Date"] = pd.to_datetime(df["Date"])

# Convert price to numeric
df["Price"] = pd.to_numeric(df["Price"])

print(df['Date'].dtype)
print(df['Product'].dtype)
print(df['Price'].dtype)