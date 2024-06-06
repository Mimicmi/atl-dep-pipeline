import requests
import json
from pprint import pprint as pp
import pandas as pd

# Config url appel api
response_profil = requests.get(
    "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/coefficients-des-profils/records")
response_temp = requests.get(
    "https://data.enedis.fr//api/explore/v2.1/catalog/datasets/donnees-de-temperature-et-de-pseudo-rayonnement/records")

# transformation en json
response_profil = response_profil.json()
response_temp = response_temp.json()

# stockage ans une variable
records_profil = response_profil["results"]
records_temp = response_temp["results"]

# création des tableaux qui vont recevoirs les données aprés les avoir trié
timestamps_profil = []
sous_profil = []
cp = []

timestamps_temp = []
trl = []
tnl = []

# boucle pour stocker les données pour chaques colonne
for record in records_profil:
    timestamps_profil.append(record["horodate"])
    sous_profil.append(record["sous_profil"])
    cp.append(record["coefficient_ajuste"])

for record in records_temp:
    timestamps_temp.append(record["horodate"])
    trl.append(record["temperature_realisee_lissee_degc"])
    tnl.append(record["temperature_normale_lissee_degc"])


# création du dataframe avec les bonnes données et les titres de colonne
df_profil = pd.DataFrame({
    "timestamp": pd.to_datetime(timestamps_profil),
    "sous_profil": sous_profil,
    "cp": pd.to_numeric(cp)
})

df_temp = pd.DataFrame({
    "timestamp": pd.to_datetime(timestamps_temp),
    "trl": pd.to_numeric(trl),
    "tnl": pd.to_numeric(tnl)
})

df_profil["sous_profil"] = df_profil["sous_profil"].astype(str)

print(df_profil.dtypes)
print(df_temp.dtypes)
