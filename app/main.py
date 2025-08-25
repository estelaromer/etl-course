import json
import os
import requests

import pandas as pd

from db import get_connection, get_engine, load_sales_data

# Extract

# CSV file
df = pd.read_csv("./data/sales.csv")
print(df.head())

# JSON file
with open("./data/users.json") as f:
    data = json.load(f)

for user in data:
    print(user["name"])

df_1 = pd.DataFrame(data)
print(df_1)

# PostgreSQL Database
conn = get_connection()

load_sales_data(conn)

query = "SELECT * FROM sales WHERE date >= '2025-01-01'"

df_2 = pd.read_sql(query, conn)
print(df_2.head())

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

# Merge sales and users info using customer ID
df["customer_id"] = df["customer_id"].astype(int)
df_1["customer_id"] = df_1["customer_id"].astype(int)
df = df.merge(df_1, on="customer_id")

# Add a total column

df["Total"] = df["Price"] * df["Quantity"]
print(df)

# Load this dataframe to a PostreSQL table
engine = get_engine()
df.to_sql("sales_cleaned", engine, if_exists="replace", index=False)

conn = get_connection()

query = "SELECT * FROM sales_cleaned"

df_3 = pd.read_sql(query, conn)
print(df_3.head())

conn.close()