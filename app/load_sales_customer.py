import pandas as pd
import os
from db import get_connection, save_dataframe

# --- Leer los CSV ---
file = os.path.abspath("data/sales.csv")
file2 = os.path.abspath("data/customers.csv")

customers = pd.read_csv(file)
sales = pd.read_csv(file2)

# --- Combinar ambos DataFrames por Customer_id ---
merged = pd.merge(sales, customers, on="Customer_id", how="left")

# --- Mostrar un resumen ---
print(merged.head())

# --- Guardar el DataFrame combinado en PostgreSQL ---
save_dataframe(merged, table_name="sales_with_customers")
