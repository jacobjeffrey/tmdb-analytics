import asyncio

from get_movies import collect_movies
from get_movie_details import collect_movie_details
from get_credits import collect_credits
from get_people import collect_people
from get_genres import collect_genres
from get_languages import collect_languages
from get_countries import collect_countries
from load_to_sql import load_to_sql

# placeholder script to ingest all required data, will refactor in a later time
async def main():  
    print("Starting TMDB data ingestion...")

    await collect_movies()
    print("Movies collected successfully")

    await collect_movie_details()
    print("Movie details collected successfully")

    await collect_credits()
    print("Credits collected successfully")

    await collect_people()
    print("People collected successfully")

    collect_genres()
    print("Genres collected successfully")

    collect_languages()
    print("Languages collected successfully")

    collect_countries()
    print("Countries collected successfully")

    load_to_sql()
    print("Ingestion complete")


if __name__ == "__main__":
    asyncio.run(main())