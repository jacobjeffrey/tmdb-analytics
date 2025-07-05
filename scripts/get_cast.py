import os
import time
from pathlib import Path

from dotenv import load_dotenv
import requests
import pandas as pd

load_dotenv()

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MOVIES_CSV = DATA_DIR / "movies.csv"
BASE_URL = "https://api.themoviedb.org/3/movie/{}/credits"
API_KEY = os.getenv("TMDB_API_KEY")

DATA_DIR.mkdir(parents=True, exist_ok=True) # ensure directory exists

# the movies.csv file needs to exist for this script to work
try:
    df_movies = pd.read_csv(MOVIES_CSV, engine="python")
    movie_ids = df_movies["id"]
except Exception:
    print("Error: you must run get_movies.py to create 'movies.csv'")

session = requests.Session()
params = {
    "api_key": API_KEY
}

cast_data = []
# get cast data
for movie_id in movie_ids:
    try:
        url = BASE_URL.format(movie_id)
        response = session.get(url, params=params)
        response.raise_for_status()
        data = response.json().get("cast")

        for cast in data:
            # filter low popularity cast, guarante to keep first 5
            if cast.get("popularity") >.5 or cast.get("order") < 5:
                cast["movie_id"] = movie_id
                cast_data.append(cast)

        time.sleep(.0333)

    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err} for movie_id {movie_id}")
        
    except Exception as e:
        print(f"Other error: {e}")



df = pd.DataFrame(cast_data)
df.to_csv(DATA_DIR / "cast.csv", index=False)