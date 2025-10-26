import pandas as pd
import os
from db import get_connection, save_dataframe

# --- Read CSVs ---
file = os.path.abspath("data/sales.csv")
file2 = os.path.abspath("data/customers.csv")

customers = pd.read_csv(file)
sales = pd.read_csv(file2)

# --- Combine dataframes by Customer_id ---
merged = pd.merge(sales, customers, on="Customer_id", how="left")

# --- show first lines ---
print(merged.head())

# --- Saved merged dataframe into PostgreSQL ---
save_dataframe(merged, table_name="sales_with_customers")
