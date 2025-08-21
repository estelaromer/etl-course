import json
import pandas as pd


# CSV file
df = pd.read_csv("./data/sales.csv")
print(df.head())

# JSON file
with open("./data/users.json") as f:
    data = json.load(f)

for user in data:
    print(user["name"])