import csv

from dotenv import load_dotenv
import pandas as pd
import aiohttp
import asyncio
from aiolimiter import AsyncLimiter

from utils import get_api_key, get_root_dir, ensure_path_exists, resolve_write_mode, chunked, fetch_api_data, serialize_json

load_dotenv()

PROJECT_ROOT = get_root_dir()
DATA_DIR = PROJECT_ROOT / "data"
CAST_CSV = DATA_DIR / "cast.csv"
MOVIES_CSV = DATA_DIR / "movies.csv"
BATCH_SIZE = 500 # how many ids to process before writing to csv
BASE_URL = "https://api.themoviedb.org/3/movie/{}/credits"
API_KEY = get_api_key()

ensure_path_exists(DATA_DIR)
# the movies.csv file needs to exist for this script to work
try:
    df_movies = pd.read_csv(MOVIES_CSV, engine="python")
    all_movie_ids = df_movies["id"]
except:
    raise FileNotFoundError("You must run get_movies.py before running this script")

# check if cast.csv exists and if should overwrite or append
options = resolve_write_mode(CAST_CSV, "movie_id", all_movie_ids)
write_mode = options["write_mode"]
ids_to_fetch = options["ids_to_fetch"]
header = options["header"]
header = (write_mode == 'w')

params = {
    "api_key": API_KEY
}

# asynchronous session options
limiter = AsyncLimiter(max_rate=30, time_period=1)
semaphore = asyncio.Semaphore(10)

# because the cast endpoint doesn't have the movie id, we need to create a wrapper
# that returns it
async def fetch_with_id(url, session, params, semaphore, limiter, movie_id,):
    data = await fetch_api_data(url, session, params, semaphore, limiter, serialize=False)
    return {"data": data, "movie_id": movie_id}

# get cast data
async def collect_cast():
    write_mode = "w" if header else "a"  # ensure local scope
    local_header = header  # avoid mutation confusion

    async with aiohttp.ClientSession() as session:
        for batch in chunked(ids_to_fetch, BATCH_SIZE):
            tasks = []
            for id in batch:
                url = BASE_URL.format(id)
                tasks.append(fetch_with_id(url, session, params, semaphore, limiter, id))
            batch_results = await asyncio.gather(*tasks)

            # now extract cast details
            cast_details = []
            for result in batch_results:
                data = result["data"]
                movie_id = result["movie_id"]
                if data:
                    cast_data = data["cast"]
                    for cast_member in cast_data:
                        cast_member["movie_id"] = movie_id
                        cast_details.append(serialize_json(cast_member))

            
                    

            # write batch to csv
            df = pd.DataFrame(cast_details)
            df.to_csv(CAST_CSV, mode = write_mode, index=False, header=local_header,
                    quoting=csv.QUOTE_ALL)
            write_mode = "a"
            local_header=False
        
if __name__ == "__main__":
    asyncio.run(collect_cast())