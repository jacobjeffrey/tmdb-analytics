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
MOVIES_FILE = DATA_DIR / "movies.parquet"
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
limiter = AsyncLimiter(max_rate=35, time_period=1)
semaphore = asyncio.Semaphore(10)


# get movie data
async def collect_movies():
    ensure_path_exists(DATA_DIR)  # ensure data directory exists

    movies_data = []
    async with aiohttp.ClientSession() as session:
        print(f"Fetching movies from {START_YEAR} to {END_YEAR}")
        for start, end in YEAR_RANGES:
            params = {
                "api_key": API_KEY,
                "primary_release_date.gte": start,
                "primary_release_date.lte": end,
                "vote_count.gte": 10,  # filter out low quality and obscure results
                "sort_by": "primary_release_date.asc", # sort to avoid pagination quirks
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

            # leave status for the very last page
            leave = (int(start[:4])==END_YEAR)
            pages = await tqdm_asyncio.gather(
                *tasks,
                total=len(tasks),          # number of pages
                desc=f"Downloading pages for year {start[:4]}",  # label for the bar
                leave=leave,
            )

            for page in pages:
                if not page:
                    continue
                page_results = page.get("results")
                if page_results:
                    for movie in page_results:
                        movies_data.append(movie)
            

    # Write to CSV after all data collected
    df = pd.DataFrame(movies_data)
    df.to_parquet(MOVIES_FILE, engine="pyarrow", compression="snappy")
    print("Movies download done")


if __name__ == "__main__":
    asyncio.run(collect_movies())
