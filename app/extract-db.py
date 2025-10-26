from db import get_engine
import pandas as pd

engine = get_engine()

query = "SELECT * FROM sales WHERE date >= '2025-01-01'"
df = pd.read_sql(query, engine)
print(df.head())
