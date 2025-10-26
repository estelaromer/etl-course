import pandas as pd
import os

file = os.path.abspath("data/sales.csv")
file2 = os.path.abspath("data/customers.csv")

df = pd.read_csv(file)
df2 = pd.read_csv(file2)

# Merge sales and customer info using customer ID
merged = pd.merge(df, df2, on="Customer_id", how="left")

print(merged.head())