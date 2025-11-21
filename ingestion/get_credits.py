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
    serialize_json,
)

load_dotenv()

PROJECT_ROOT = get_root_dir()
DATA_DIR = PROJECT_ROOT / "data"
CREDITS_CSV = DATA_DIR / "credits.csv"
MOVIES_CSV = DATA_DIR / "movies.csv"
BATCH_SIZE = 500  # how many ids to process before writing to csv
BASE_URL = "https://api.themoviedb.org/3/movie/{}/credits"
API_KEY = get_api_key()

params = {
    "api_key": API_KEY,
}

# asynchronous session options (these are fine as globals)
limiter = AsyncLimiter(max_rate=40, time_period=1)
semaphore = asyncio.Semaphore(10)


# because the credits endpoint doesn't have the movie id, we need to create a wrapper
# that returns it
async def fetch_with_id(url, session, params, semaphore, limiter, movie_id):
    data = await fetch_api_data(
        url,
        session,
        params,
        semaphore,
        limiter,
        serialize=False,
    )
    return {"data": data, "movie_id": movie_id}


# get credits data
async def collect_credits():
    # 👉 everything that depends on movies.csv existing lives *inside* this function
    ensure_path_exists(DATA_DIR)

    # the movies.csv file needs to exist for this script to work
    try:
        df_movies = pd.read_csv(MOVIES_CSV, engine="python")
        all_movie_ids = df_movies["id"]
    except FileNotFoundError:
        raise FileNotFoundError("You must run get_movies.py before running this script")

    # check if credits.csv exists and if we should overwrite or append
    options = resolve_write_mode(CREDITS_CSV, "movie_id", all_movie_ids)
    write_mode = options["write_mode"]
    ids_to_fetch = options["ids_to_fetch"]
    local_header = options["header"]

    # nothing to do
    if not len(ids_to_fetch):
        print("No new movie IDs to fetch credits for.")
        return

    async with aiohttp.ClientSession() as session:
        for batch in chunked(ids_to_fetch, BATCH_SIZE):
            tasks = []
            for movie_id in batch:
                url = BASE_URL.format(movie_id)
                tasks.append(
                    fetch_with_id(
                        url,
                        session,
                        params,
                        semaphore,
                        limiter,
                        movie_id,
                    )
                )
            batch_results = await asyncio.gather(*tasks)

            # extract credits details
            credits_rows = []
            for result in batch_results:
                data = result["data"]
                movie_id = result["movie_id"]
                if data and "cast" in data:
                    for cast_member in data["cast"]:
                        cast_member["movie_id"] = movie_id
                        credits_rows.append(serialize_json(cast_member))

            if not credits_rows:
                continue

            # write batch to csv
            df = pd.DataFrame(credits_rows)
            df.to_csv(
                CREDITS_CSV,
                mode=write_mode,
                index=False,
                header=local_header,
                quoting=csv.QUOTE_ALL,
            )

            # after first write, always append without header
            write_mode = "a"
            local_header = False


if __name__ == "__main__":
    asyncio.run(collect_credits())
