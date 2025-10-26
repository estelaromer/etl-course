import requests

response = requests.get("https://raw.githubusercontent.com/estelaromer/csv-examples/refs/heads/main/data.json")
data = response.json()

print(data["company"]["name"])