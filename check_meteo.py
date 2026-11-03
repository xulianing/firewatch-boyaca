import json
import pandas as pd

ruta = "data/raw/meteo/_test/tunja_2025_era5.json"

d = json.load(open(ruta, encoding="utf-8"))

h = pd.DataFrame(d["hourly"])

print("Filas:", len(h))
print("\nNulos:")
print(h.isna().sum())