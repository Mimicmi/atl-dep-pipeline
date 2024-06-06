import requests
import json
from pprint import pprint as pp

response = requests.get(
    "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/coefficients-des-profils/records")
pp(response.json())
