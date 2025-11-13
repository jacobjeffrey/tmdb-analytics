import os
import time
from pathlib import Path

from dotenv import load_dotenv
import requests
import pandas as pd

from utils import get_root_dir

load_dotenv()

PROJECT_ROOT = get_root_dir()
DATA_DIR = PROJECT_ROOT / "data"
GENRES_CSV = PROJECT_ROOT / "data" / "genres.csv"
os.makedirs(os.path.dirname(GENRES_CSV), exist_ok=True) # ensure path exists

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
df = pd.DataFrame(genres)
df.to_csv(GENRES_CSV, index=False)