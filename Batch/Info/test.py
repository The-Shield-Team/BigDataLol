import json
import random
import pandas as pd

# Load the JSON file
with open('./all_champ_storege.json', encoding='utf-8') as f:
    # dummy data
    data = json.load(f)

puid = random.randint(1, 1000)
file_name = 'all_champ_storege.json'
rows = []
for key, value in data.items():
    rows.append({
        'puid': puid,
        'clave': key,
        'valor': value,
        'nombre del archivo': file_name
    })

# Crear un DataFrame de pandas
df = pd.DataFrame(rows)

# Mostrar el DataFrame
print(df)

