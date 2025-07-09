import os
import time
from pathlib import Path
import json
import csv

from dotenv import load_dotenv
import requests
import pandas as pd
import aiohttp
import asyncio
from aiolimiter import AsyncLimiter

load_dotenv()

current_file = Path(__file__).resolve()
project_root = current_file.parents[1]
data_path = project_root / "data" / "movie_details.csv"
os.makedirs(os.path.dirname(data_path), exist_ok=True) # ensure path exists

# the movies.csv file needs to exist for this script to work
movies_path = project_root / "data" / "movies.csv"
try:
    df_movies = pd.read_csv(movies_path, engine="python")
    movie_ids = df_movies["id"]
except:
    print("Error, you must run get movies.csv before running this script.")
    print("Run get_moves.py")


api_key = os.getenv("TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3/movie/"
params = {
    "api_key": api_key
}

limiter = AsyncLimiter(max_rate=35, time_period=1)

async def fetch_movie_details(session, movie_id):
    url = f"{BASE_URL}{movie_id}"
    async with limiter:
        async with session.get(url, params=params) as response:
            try:
                response.raise_for_status()
                data = await response.json()
                for key, value in data.items():
                    if isinstance(value, (list, dict)):
                        data[key] = json.dumps(value)
            except aiohttp.ClientResponseError as e:
                raise
    return data

async def collect_movie_details():
    async with aiohttp.ClientSession() as session:
        movie_details = [fetch_movie_details(session, movie_id) for movie_id in movie_ids]
        results = await asyncio.gather(*movie_details)
    return results

movie_details = asyncio.run(collect_movie_details())

df = pd.DataFrame(movie_details)
df.to_csv(data_path, index=False, quoting=csv.QUOTE_ALL)