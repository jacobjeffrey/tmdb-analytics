import os
import csv

from dotenv import load_dotenv
import requests
import pandas as pd

from utils import get_root_dir

load_dotenv()

PROJECT_ROOT = get_root_dir()
DATA_DIR = PROJECT_ROOT / "data"
SEEDS_DIR = PROJECT_ROOT / "dbt" / "seeds"
GENRES_CSV = SEEDS_DIR / "genres.csv"
COUNTRIES_CSV = SEEDS_DIR / "countries.csv"
LANGUAGES_CSV = SEEDS_DIR / "languages.csv"

api_key = os.getenv("TMDB_API_KEY")

genres_url = "https://api.themoviedb.org/3/genre/movie/list"
countries_url = "https://api.themoviedb.org/3/configuration/countries"
languages_url = "https://api.themoviedb.org/3/configuration/languages"

session = requests.Session()
params = {
    "api_key": api_key,
}


# get seed data
def update_seeds():
    # genres
    genres_response = session.get(genres_url, params=params)
    genres_response.raise_for_status()
    genres_data = genres_response.json()["genres"]

    # countries
    countries_response = session.get(countries_url, params=params)
    countries_response.raise_for_status()
    countries_data = countries_response.json()

    # languages
    languages_response = session.get(languages_url, params=params)
    languages_response.raise_for_status()
    languages_data = languages_response.json()

    # write to csv
    df = pd.DataFrame(genres_data)
    df.to_csv(GENRES_CSV, index=False, quoting=csv.QUOTE_ALL)

    df = pd.DataFrame(countries_data)
    df.to_csv(COUNTRIES_CSV, index=False, quoting=csv.QUOTE_ALL)

    df = pd.DataFrame(languages_data)
    df.to_csv(LANGUAGES_CSV, index=False, quoting=csv.QUOTE_ALL)


if __name__ == "__main__":
    update_seeds()