import pandas as pd
import os

file = os.path.abspath("data/sales.csv")

df = pd.read_csv(file)

# Remove duplicate rows
df = df.drop_duplicates()

# Fill missing prices with 0
df["Price"] = df["Price"].fillna(0)

# Make all product names lowercase for consistency
df["Product"] = df["Product"].str.lower()

print(df.head())