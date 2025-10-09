import os
import time
from pathlib import Path
import csv

from dotenv import load_dotenv
import requests
import pandas as pd

load_dotenv()

current_file = Path(__file__).resolve()
project_root = current_file.parents[1]
data_path = project_root / "data" / "countries.csv"
os.makedirs(os.path.dirname(data_path), exist_ok=True) # ensure path exists

api_key = os.getenv("TMDB_API_KEY")
url = "https://api.themoviedb.org/3/configuration/countries"
session = requests.Session()

params = {
    "api_key": api_key,
}

# get genre data
response = session.get(url, params=params)
response.raise_for_status()
data = response.json()
genres = response.json()

# write to csv
df = pd.DataFrame(genres)
df.to_csv(data_path, index=False, quoting=csv.QUOTE_ALL)