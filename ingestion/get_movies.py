import os
import time
import json
from pathlib import Path
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
MOVIES_CSV = DATA_DIR / "movies.csv"
API_KEY = get_api_key()
BASE_URL = "https://api.themoviedb.org/3/discover/movie"

# because TMDB caps pagination at 500, we need to pull data for each year individually
START_YEAR = 2000
END_YEAR = 2025
YEAR_RANGES = [
    (f"{year}-01-01", f"{year}-12-31")
    for year in range(START_YEAR, END_YEAR + 1)
]

# asynchronous session options
limiter = AsyncLimiter(max_rate=30, time_period=1)
semaphore = asyncio.Semaphore(10)


# get movie data
async def collect_movies():
    ensure_path_exists(DATA_DIR)  # ensure data directory exists

    movies_data = []
    async with aiohttp.ClientSession() as session:
        for start, end in YEAR_RANGES:
            params = {
                "api_key": API_KEY,
                "primary_release_date.gte": start,
                "primary_release_date.lte": end,
                "vote_count.gte": 10,  # filter out low quality and obscure results
            }

            first_page_data = await fetch_api_data(
                BASE_URL,
                session,
                params,
                semaphore,
                limiter,
                serialize=False,
            )
            max_pages = min(first_page_data.get("total_pages", 1), 500)

            tasks = []
            for page in range(1, max_pages + 1):
                page_params = params.copy()
                page_params["page"] = page
                tasks.append(
                    fetch_api_data(
                        BASE_URL,
                        session,
                        page_params,
                        semaphore,
                        limiter,
                        serialize=False,
                    )
                )

            pages = await asyncio.gather(*tasks)
            for page in pages:
                if not page:
                    continue
                page_results = page.get("results")
                if page_results:
                    for movie in page_results:
                        movies_data.append(serialize_json(movie))

    # Write to CSV after all data collected
    df = pd.DataFrame(movies_data)
    df.to_csv(MOVIES_CSV, index=False, quoting=csv.QUOTE_ALL)


if __name__ == "__main__":
    asyncio.run(collect_movies())
