import time

import asyncio

from get_movies import collect_movies
from get_movie_details import collect_movie_details
from get_credits import collect_credits
from load_to_sql import load_to_sql

# placeholder script to ingest all required data, will refactor in a later time
async def main():  
    start = time.time()
    print("Starting TMDB data ingestion...")
    await collect_movies()
    await collect_movie_details()
    await collect_credits()

    # print("Loading to SQL")
    # load_to_sql()
    # print("Ingestion complete")
    end = time.time()
    elapsed_minutes = (end - start) / 60

    print(f"Elapsed time: {elapsed_minutes:.2f} minutes")


if __name__ == "__main__":
    asyncio.run(main())