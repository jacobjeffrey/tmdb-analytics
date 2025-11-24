from datetime import datetime, timezone

from dotenv import load_dotenv
import pandas as pd
import aiohttp
import asyncio
from aiolimiter import AsyncLimiter
from tqdm.asyncio import tqdm_asyncio

from utils import (
    get_api_key,
    get_root_dir,
    ensure_path_exists,
    fetch_api_data,
)

load_dotenv()

PROJECT_ROOT = get_root_dir()
DATA_DIR = PROJECT_ROOT / "data"
CREDITS_FILE = DATA_DIR / "credits.parquet"
MOVIES_FILE = DATA_DIR / "movies.parquet"
BASE_URL = "https://api.themoviedb.org/3/movie/{}/credits"
API_KEY = get_api_key()

params = {
    "api_key": API_KEY,
}

# asynchronous session options
limiter = AsyncLimiter(max_rate=35, time_period=1)
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
    return {"movie_id": movie_id, 
            "payload_json": data, 
            "ingested_at": datetime.now(timezone.utc)
            }


# get credits data
async def collect_credits():
    ensure_path_exists(DATA_DIR)

    # the movies.parquet file needs to exist for this script to work
    try:
        df_movies = pd.read_parquet(MOVIES_FILE, engine="pyarrow")
        all_movie_ids = df_movies["id"]
    except FileNotFoundError:
        raise FileNotFoundError("You must run get_movies.py before running this script")

    async with aiohttp.ClientSession() as session:
        tasks = []
        for movie_id in all_movie_ids:
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
        print("Fetching credits")
        batch_results = await tqdm_asyncio.gather(
            *tasks,
            total=len(tasks)
        )

        # extract credits details
        results = []
        for result in batch_results:            
            if result["credits_json"]:
                results.append(result)


        # write batch to Parquet
        df = pd.DataFrame(results)
        df.to_parquet(CREDITS_FILE, engine="pyarrow",)
        print("Credits downloaded")


if __name__ == "__main__":
    asyncio.run(collect_credits())
