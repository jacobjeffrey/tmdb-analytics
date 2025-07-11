import csv

from dotenv import load_dotenv
import pandas as pd
import aiohttp
import asyncio
from aiolimiter import AsyncLimiter

from utils import get_api_key, get_root_dir, ensure_path_exists, resolve_write_mode, chunked, fetch_api_data

load_dotenv()

project_root = get_root_dir()
DATA_PATH = project_root / "data"
ensure_path_exists(DATA_PATH)
MOVIE_DETAILS_CSV = DATA_PATH / "movie_details.csv"
movies_path = DATA_PATH / "movies.csv"
BATCH_SIZE = 500 # how many ids to process before writing to csv

# the movies.csv file needs to exist for this script to work
try:
    df_movies = pd.read_csv(movies_path, engine="python")
    all_movie_ids = df_movies["id"]
except:
    raise FileNotFoundError("You must run get_movies.py before running this script")

# check if movie_details.csv exists and if should overwrite or append
options = resolve_write_mode(MOVIE_DETAILS_CSV, "id", all_movie_ids)
write_mode = options["write_mode"]
ids_to_fetch = options["ids_to_fetch"]
header = options["header"]

# prepare API url and parameters
api_key = get_api_key()
BASE_URL = "https://api.themoviedb.org/3/movie/"
params = {
    "api_key": api_key
}

# asynchronous session options
limiter = AsyncLimiter(max_rate=30, time_period=1)
semaphore = asyncio.Semaphore(10)

async def collect_movie_details():
    write_mode = "w" if header else "a"  # ensure local scope
    local_header = header  # avoid mutation confusion

    async with aiohttp.ClientSession() as session:
        for i, batch in enumerate(chunked(ids_to_fetch, BATCH_SIZE)):
            print(f"Fetching batch {i} of length {len(batch)}")
            tasks = []
            for id in batch:
                url = BASE_URL + str(id)
                tasks.append(fetch_api_data(url, session, params, semaphore, limiter))
            results = await asyncio.gather(*tasks)

            # write batch to csv
            df = pd.DataFrame(results)
            df.to_csv(MOVIE_DETAILS_CSV, mode = write_mode, index=False, header=local_header,
                    quoting=csv.QUOTE_ALL)
            write_mode = "a"
            local_header=False
        
if __name__ == "__main__":
    asyncio.run(collect_movie_details())