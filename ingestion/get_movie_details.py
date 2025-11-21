import csv

from dotenv import load_dotenv
import pandas as pd
import aiohttp
import asyncio
from aiolimiter import AsyncLimiter

from utils import (
    get_api_key,
    get_root_dir,
    ensure_path_exists,
    resolve_write_mode,
    chunked,
    fetch_api_data,
)

load_dotenv()

PROJECT_ROOT = get_root_dir()
DATA_DIR = PROJECT_ROOT / "data"
MOVIE_DETAILS_CSV = DATA_DIR / "movie_details.csv"
MOVIES_CSV = DATA_DIR / "movies.csv"
BATCH_SIZE = 500  # how many ids to process before writing to csv
BASE_URL = "https://api.themoviedb.org/3/movie/"

# prepare API url and parameters
API_KEY = get_api_key()
params = {"api_key": API_KEY}

# asynchronous session options
limiter = AsyncLimiter(max_rate=30, time_period=1)
semaphore = asyncio.Semaphore(10)


async def collect_movie_details():
    ensure_path_exists(DATA_DIR)

    # the movies.csv file needs to exist for this script to work
    try:
        df_movies = pd.read_csv(MOVIES_CSV, engine="python")
        all_movie_ids = df_movies["id"]
    except FileNotFoundError:
        raise FileNotFoundError("You must run get_movies.py before running this script")

    # check if movie_details.csv exists and if we should overwrite or append
    options = resolve_write_mode(MOVIE_DETAILS_CSV, "id", all_movie_ids)
    write_mode = options["write_mode"]
    ids_to_fetch = options["ids_to_fetch"]
    local_header = options["header"]

    if not len(ids_to_fetch):
        print("No new movie IDs to fetch details for.")
        return

    async with aiohttp.ClientSession() as session:
        for batch in chunked(ids_to_fetch, BATCH_SIZE):
            tasks = []
            for movie_id in batch:
                url = BASE_URL + str(movie_id)
                tasks.append(
                    fetch_api_data(
                        url,
                        session,
                        params,
                        semaphore,
                        limiter,
                    )
                )
            results = await asyncio.gather(*tasks)

            # write batch to csv
            df = pd.DataFrame(results)
            df.to_csv(
                MOVIE_DETAILS_CSV,
                mode=write_mode,
                index=False,
                header=local_header,
                quoting=csv.QUOTE_ALL,
            )

            # after first batch, always append without header
            write_mode = "a"
            local_header = False


if __name__ == "__main__":
    asyncio.run(collect_movie_details())
