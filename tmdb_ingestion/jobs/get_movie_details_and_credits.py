import os
from datetime import datetime, timezone

from dotenv import load_dotenv
import pandas as pd
import aiohttp
import asyncio
from aiolimiter import AsyncLimiter
from tqdm import tqdm
import pyarrow as pa
import pyarrow.parquet as pq

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
API_KEY = get_api_key()
BATCH_SIZE = 500

# prepare API url and parameters
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
    
    # if an old output file exists, remove it (we'll rewrite from scratch)
    if os.path.exists(MOVIES_DETAILS_AND_CREDITS_FILE):
        os.remove(MOVIES_DETAILS_AND_CREDITS_FILE)

    writer = None  # pyarrow ParquetWriter, created lazily on first batch
    
    async with aiohttp.ClientSession() as session:
        num_movies = len(all_movie_ids)
        num_batches = (num_movies + BATCH_SIZE - 1) // BATCH_SIZE
        print(
            f"Fetching movie details and credits "
            f"in {num_batches} batches (batch size = {BATCH_SIZE})"
        )

        tasks = []
        with tqdm(total=num_batches, desc="Batches", unit="batch") as pbar:
            for batch_start in range(0, num_movies, BATCH_SIZE):
                batch_ids = all_movie_ids[batch_start : batch_start + BATCH_SIZE]

                # create tasks just for this batch
                tasks = []
                for movie_id in batch_ids:
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

                # run this batch (semaphore + limiter still control concurrency)
                batch_results = await asyncio.gather(*tasks)

                # turn batch into DataFrame
                df_batch = pd.DataFrame(batch_results)

                # streaming write to Parquet via pyarrow
                table = pa.Table.from_pandas(df_batch)

                if writer is None:
                    writer = pq.ParquetWriter(
                        MOVIES_DETAILS_AND_CREDITS_FILE,
                        table.schema,
                        compression="snappy",
                    )

                writer.write_table(table)

                # update batch progress bar
                pbar.update(1)

    # close the Parquet writer once all batches are processed
    if writer is not None:
        writer.close()


if __name__ == "__main__":
    asyncio.run(collect_movie_details_and_credits())
