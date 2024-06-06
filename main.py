import requests
import json
from pprint import pprint as pp
import pandas as pd

response = requests.get(
    "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/coefficients-des-profils/records")

response_json = response.json()
df = pd.DataFrame.from_dict(response_json["results"])

print(df)
