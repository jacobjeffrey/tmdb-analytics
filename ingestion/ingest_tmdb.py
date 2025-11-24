import time

import asyncio

from get_movies import collect_movies
from get_movie_details_and_credits import collect_movie_details_and_credits

# placeholder script to ingest all required data, will refactor in a later time
async def main():  
    start = time.time()
    print("Starting TMDB data ingestion...")
    await collect_movies()
    await collect_movie_details_and_credits()
    end = time.time()
    elapsed_minutes = (end - start) / 60

    print(f"Elapsed time: {elapsed_minutes:.2f} minutes")


if __name__ == "__main__":
    asyncio.run(main())