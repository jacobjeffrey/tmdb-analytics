import os
import time
from pathlib import Path
import csv

from dotenv import load_dotenv
import requests
import pandas as pd

from utils import get_root_dir

load_dotenv()

PROJECT_ROOT = get_root_dir()
DATA_DIR = PROJECT_ROOT / "data"
LANGUAGES_CSV = PROJECT_ROOT / "data" / "languages.csv"
os.makedirs(os.path.dirname(LANGUAGES_CSV), exist_ok=True) # ensure path exists

api_key = os.getenv("TMDB_API_KEY")
url = "https://api.themoviedb.org/3/configuration/languages"
session = requests.Session()

params = {
    "api_key": api_key,
}

# get languages
def collect_languages():
    response = session.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    genres = response.json()

    # write to csv
    df = pd.DataFrame(genres)
    df.to_csv(LANGUAGES_CSV, index=False, quoting=csv.QUOTE_ALL)

if __name__ == "__main__":
    collect_languages()