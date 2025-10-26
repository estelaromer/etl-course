import json
import os

file = os.path.abspath("data/users.json")
with open(file) as f:
    data = json.load(f)

for user in data:
    print(user["name"])