import requests
import json
from pprint import pprint as pp
import pandas as pd

# Config url appel api
response = requests.get("https://data.enedis.fr/api/explore/v2.1/catalog/datasets/coefficients-des-profils/records")
response_temp = requests.get("https://data.enedis.fr//api/explore/v2.1/catalog/datasets/donnees-de-temperature-et-de-pseudo-rayonnement/records")

# transformation en json
response= response.json()
response_temp= response_temp.json()

# stockage ans une variable
records = response_temp["results"]

# création des tableaux qui vont recevoirs les données aprés les avoir trié
timestamps = []
trl = []
tnl = []

# boucle pour stocker les données pour chaques colonne
for record in records:
    timestamps.append(record["horodate"])
    trl.append(record["temperature_realisee_lissee_degc"])
    tnl.append(record["temperature_normale_lissee_degc"])

# création du dataframe avec les bonnes données et les titres de colonne
df = pd.DataFrame({
    "timestamp": timestamps,
    "trl": trl,
    "tnl": tnl
})

print(df)
