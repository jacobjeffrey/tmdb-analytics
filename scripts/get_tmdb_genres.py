import os
import time
from pathlib import Path

from dotenv import load_dotenv
import requests
import pandas

load_dotenv()

current_file = Path(__file__).resolve()
project_root = current_file.parents[1]
data_path = project_root / "data" / "genres.csv"

api_key = os.getenv("TMDB_API_KEY")
url = "https://api.themoviedb.org/3/genre/movie/list"
session = requests.Session()

params = {
    "api_key": api_key,
}

# get genre data
response = session.get(url, params=params)
response.raise_for_status()
data = response.json()
genres = response.json()["genres"]

# write to csv
df = pandas.DataFrame(genres)
df.to_csv(data_path, index=False)