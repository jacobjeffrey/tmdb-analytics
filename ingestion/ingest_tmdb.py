import time

import asyncio

from get_movies import collect_movies
from get_movie_details import collect_movie_details
from get_credits import collect_credits
from get_genres import collect_genres
from get_languages import collect_languages
from get_countries import collect_countries
from load_to_sql import load_to_sql

# placeholder script to ingest all required data, will refactor in a later time
async def main():  
    start = time.time()
    print("Starting TMDB data ingestion...")

    print("Collecting movies")
    await collect_movies()
    print("Movies collected successfully")

    print("Collecting movie details")
    await collect_movie_details()
    print("Movie details collected successfully")

    print("Collecting credts")
    await collect_credits()
    print("Credits collected successfully")

    print("Collecting genres")
    collect_genres()
    print("Genres collected successfully")

    print("Collecting languages")
    collect_languages()
    print("Languages collected successfully")

    print("Collecting countries")
    collect_countries()
    print("Countries collected successfully")

    print("Loading to SQL")
    load_to_sql()
    print("Ingestion complete")
    end = time.time()
    elapsed_minutes = (end - start) / 60

    print(f"Elapsed time: {elapsed_minutes:.2f} minutes")


if __name__ == "__main__":
    asyncio.run(main())