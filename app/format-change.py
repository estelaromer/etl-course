import pandas as pd
import os

file = os.path.abspath("data/sales.csv")

df = pd.read_csv(file)

print(df.dtypes)

# Convert string to datetime
df["Date"] = pd.to_datetime(df["Date"])

# Convert price to numeric
df["Price"] = pd.to_numeric(df["Price"])

print(df.dtypes)