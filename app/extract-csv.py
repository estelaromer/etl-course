import pandas as pd
import os

file = os.path.abspath("data/sales.csv")
df = pd.read_csv(file)

print(df.head())