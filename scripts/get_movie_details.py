import os
import time
from pathlib import Path
import json

from dotenv import load_dotenv
import requests
import pandas as pd

load_dotenv()

current_file = Path(__file__).resolve()
project_root = current_file.parents[1]
data_path = project_root / "data" / "movie_details.csv"
# the movies.csv file needs to exist for this script to work
movies_path = project_root / "data" / "movies.csv"
os.makedirs(os.path.dirname(data_path), exist_ok=True) # ensure path exists

api_key = os.getenv("TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3/movie/"
session = requests.Session()
movie_details = []

params = {
    "api_key": api_key
}

# get the movie ids
df_movies = pd.read_csv(movies_path, engine="python")
movie_ids = df_movies["id"]

# get movie data
for id in movie_ids:
    try:
        url = f"{BASE_URL}{id}"
        response = session.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        # serialize data
        for key, value in data.items():
            if isinstance(value, (list, dict)):
                data[key] = json.dumps(value)
        
        movie_details.append(data)
        time.sleep(.0333)
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error: {err} for movie_id {id}")
    except Exception as e:
        print(f"Other error: {e}")



df = pd.DataFrame(movie_details)
df.to_csv(data_path, index=False)