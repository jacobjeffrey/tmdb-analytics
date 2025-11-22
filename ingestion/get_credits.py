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
    chunked,
    fetch_api_data,
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
limiter = AsyncLimiter(max_rate=30, time_period=1)
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

    # need to create the file manually since the response here won't have natural headers
    df_empty = pd.DataFrame(columns=["movie_id", "credits_data"])
    df_empty.to_csv(CREDITS_CSV, index=False)

    async with aiohttp.ClientSession() as session:
        for batch in chunked(all_movie_ids, BATCH_SIZE):
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
            results = []
            for result in batch_results:
                movie_id = result["movie_id"]
                data = result["data"]
                
                if data:
                    results.append([movie_id,data])


            # write batch to csv
            df = pd.DataFrame(results)
            df.to_csv(
                CREDITS_CSV,
                mode='a',
                index=False,
                header=False,
                quoting=csv.QUOTE_ALL,
            )


if __name__ == "__main__":
    asyncio.run(collect_credits())
