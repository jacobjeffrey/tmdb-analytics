import os
import time
from pathlib import Path
import json
import csv

from dotenv import load_dotenv
import pandas as pd
import aiohttp
import asyncio
from aiolimiter import AsyncLimiter
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

load_dotenv()

current_file = Path(__file__).resolve()
project_root = current_file.parents[1]
DATA_PATH = project_root / "data"
MOVIE_DETAILS_CSV = DATA_PATH / "movie_details.csv"
os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True) # ensure path exists
BATCH_SIZE = 500 # how many ids to process before writing to csv

# the movies.csv file needs to exist for this script to work
movies_path = project_root / "data" / "movies.csv"
try:
    df_movies = pd.read_csv(movies_path, engine="python")
    all_movie_ids = df_movies["id"]
except:
    print("Error, you must run get movies.csv before running this script.")
    exit(1)

# check if movie_details.csv exists
if MOVIE_DETAILS_CSV.exists():
    overwrite = input("cast.csv exists, overwrite? (y/n) ")
    if overwrite.lower() == 'y':
        write_mode = 'w'
        movies_to_fetch = all_movie_ids
    else:
        write_mode = 'a'
        movies_seen = pd.read_csv(MOVIE_DETAILS_CSV)["movie_id"]
        movies_to_fetch = list(all_movie_ids.difference(movies_seen))
else:
    write_mode = 'w'
    movies_to_fetch = all_movie_ids

header = (write_mode == 'w')

api_key = os.getenv("TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3/movie/"
params = {
    "api_key": api_key
}

# asynchronous session options
limiter = AsyncLimiter(max_rate=30, time_period=1)
semaphore = asyncio.Semaphore(10)

def notify_before_retry(retry_state):
    print(f"Retrying movie {retry_state.args[1]}: attempt {retry_state.attempt_number}")

# def batch function, to be put into utils later
def chunked(iterable, batch_size):
    try:
        iterable = list(iterable)
        for i in range(0, len(iterable), batch_size):
            yield iterable[i:i + batch_size]
    except:
        raise TypeError("iterable could not be converted to list")

@retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        before_sleep = notify_before_retry
)
async def fetch_movie_details(session, movie_id):
    url = f"{BASE_URL}{movie_id}"
    async with semaphore:
        async with limiter:
            async with session.get(url, params=params) as response:
                try:
                    response.raise_for_status()
                    data = await response.json()
                    for key, value in data.items():
                        if isinstance(value, (list, dict)):
                            data[key] = json.dumps(value)
                    print(f"Fetched movie {movie_id} at {time.strftime('%H:%M:%S.%f')[:-3]}")
                except aiohttp.ClientResponseError as e:
                    raise
    return data

async def collect_movie_details():
    write_mode = "w" if header else "a"  # ensure local scope
    local_header = header  # avoid mutation confusion
    async with aiohttp.ClientSession() as session:
        for batch in chunked(movies_to_fetch, BATCH_SIZE):
            results = await asyncio.gather(
                *[fetch_movie_details(session, id) for id in batch]
            )

            df = pd.DataFrame(results)
            df.to_csv(MOVIE_DETAILS_CSV, mode = write_mode, index=False, header=local_header,
                    quoting=csv.QUOTE_ALL)
            write_mode = "a"
            local_header=False
        

