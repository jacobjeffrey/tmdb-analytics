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
MOVIES_DETAILS_AND_CREDITS_FILE = DATA_DIR / "movie_details_and_credits" / "movie_details_and_credits.parquet"
MOVIES_FILE = DATA_DIR / "movies/movies.parquet"
BASE_URL = "https://api.themoviedb.org/3/movie/"

# prepare API url and parameters
API_KEY = get_api_key()
params = {"api_key": API_KEY,
          "append_to_response": "credits", # add credits to the payload
          }

# asynchronous session options
limiter = AsyncLimiter(max_rate=35, time_period=1)
semaphore = asyncio.Semaphore(10)

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

async def collect_movie_details_and_credits():
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
            url = BASE_URL + str(movie_id)
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

        print("Fetching movie details and credits")
        results = await tqdm_asyncio.gather(
            *tasks,
            total=len(tasks),
        )

        # write batch to Parquet
        df = pd.DataFrame(results)
        df.to_parquet(MOVIES_DETAILS_AND_CREDITS_FILE, engine="pyarrow", compression="snappy",)
        print("Movie details and credits downloaded")


if __name__ == "__main__":
    asyncio.run(collect_movie_details_and_credits())
