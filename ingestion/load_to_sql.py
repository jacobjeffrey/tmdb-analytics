import os

import psycopg2
import pandas as pd
from dotenv import load_dotenv

from utils import get_root_dir

PROJECT_ROOT = get_root_dir()
DATA_DIR = PROJECT_ROOT / "data"
MOVIES_CSV = DATA_DIR / "movies.csv"
MOVIE_DETAILS_CSV = DATA_DIR / "movie_details.csv"
CREDITS_CSV = DATA_DIR / "credits.csv"
PEOPLE_CSV = DATA_DIR / "people.csv"
GENRES_CSV = DATA_DIR / "genres.csv"
COUNTRIES_CSV = DATA_DIR / "countries.csv"
LANGUAGES_CSV = DATA_DIR / "languages.csv"

# get database settings
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def load_to_sql():
    conn = psycopg2.connect(database=DB_NAME,
                        user=DB_USER,
                        host=DB_HOST,
                        password=DB_PASSWORD,
                        port=DB_PORT)
    cur = conn.cursor()
    # update movies table
    with open(MOVIES_CSV) as file:
        cur.execute("TRUNCATE raw.movies")
        cur.copy_expert(
            """
            COPY raw.movies (
                adult, 
                backdrop_path, 
                genre_ids, 
                id,
                original_language, 
                original_title, 
                overview,
                popularity, 
                poster_path, 
                release_date, 
                title,
                video, 
                vote_average, 
                vote_count
            )
            FROM STDIN
            WITH (
                FORMAT csv,
                HEADER TRUE,
                QUOTE '"',
                ESCAPE '"',
                NULL ''
            )
            """,
            file
        )

    conn.commit()
    print("raw.movies updated successfully")

    # update movie_details table
    with open(MOVIE_DETAILS_CSV) as file:
        cur.execute("TRUNCATE raw.movie_details")
        cur.copy_expert(
            """
            COPY raw.movie_details (
                adult, 
                backdrop_path, 
                belongs_to_collection, 
                budget, 
                genres, 
                homepage,
                id, imdb_id,
                origin_country,
                original_language,
                original_title,
                overview,
                popularity,
                poster_path,
                production_companies,
                production_countries,
                release_date,
                revenue,
                runtime,
                spoken_languages,
                status,
                tagline,
                title,
                video,
                vote_average,
                vote_count
            )
            FROM STDIN
            WITH (
                FORMAT csv,
                HEADER TRUE,
                QUOTE '"',
                ESCAPE '"',
                NULL ''
            )
            """,
            file
        )
    print("raw.movie_details updated successfully")
    conn.commit()

    # CREDITS_CSV -> raw.credits
    with open(CREDITS_CSV) as file:
        cur.execute("TRUNCATE raw.credits")
        cur.copy_expert(
            """
            COPY raw.credits (
                adult,
                gender,
                id,
                known_for_department,
                name,
                original_name,
                popularity,
                profile_path,
                cast_id,
                character,
                credit_id,
                "order",
                movie_id
            )
            FROM STDIN
            WITH (
                FORMAT csv,
                HEADER TRUE,
                QUOTE '"',
                ESCAPE '"',
                NULL ''
            )
            """,
            file
        )
    conn.commit()
    print("raw.credits updated successfully")

    with open(GENRES_CSV) as file:
        cur.execute("TRUNCATE raw.genres")
        cur.copy_expert(
            """
            COPY raw.genres (
                id,
                name
            )
            FROM STDIN
            WITH (
                FORMAT csv,
                HEADER TRUE,
                QUOTE '"',
                ESCAPE '"',
                NULL ''
            )
            """,
            file
        )

    print("raw.genres updated successfully")
    conn.commit()

    # countries table
    with open(COUNTRIES_CSV) as file:
        cur.execute("TRUNCATE raw.countries")
        cur.copy_expert(
            """
            COPY raw.countries (
                iso_3166_1,
                english_name,
                native_name
            )
            FROM STDIN
            WITH (
                FORMAT csv,
                HEADER TRUE,
                QUOTE '"',
                ESCAPE '"',
                NULL ''
            )
            """,
            file
        )

    print("raw.countries updated successfully")
    conn.commit()

    # languages table
    with open(LANGUAGES_CSV) as file:
        cur.execute("TRUNCATE raw.languages")
        cur.copy_expert(
            """
            COPY raw.languages (
                iso_639_1,
                english_name,
                name
            )
            FROM STDIN
            WITH (
                FORMAT csv,
                HEADER TRUE,
                QUOTE '"',
                ESCAPE '"',
                NULL ''
            )
            """,
            file
        )
    conn.commit()
    print("raw.languages updated successfully")

    # CREDITS_CSV -> raw.credits
    with open(CREDITS_CSV) as file:
        cur.execute("TRUNCATE raw.credits")
        cur.copy_expert(
            """
            COPY raw.credits (
                adult,
                gender,
                id,
                known_for_department,
                name,
                original_name,
                popularity,
                profile_path,
                cast_id,
                character,
                credit_id,
                "order",
                movie_id
            )
            FROM STDIN
            WITH (
                FORMAT csv,
                HEADER TRUE,
                QUOTE '"',
                ESCAPE '"',
                NULL ''
            )
            """,
            file
        )

    conn.commit()

    # PEOPLE_CSV -> raw.people
    with open(PEOPLE_CSV) as file:
        cur.execute("TRUNCATE raw.people")
        cur.copy_expert(
            """
            COPY raw.people (
                adult,
                also_known_as,
                biography,
                birthday,
                deathday,
                gender,
                homepage,
                id,
                imdb_id,
                known_for_department,
                name,
                place_of_birth,
                popularity,
                profile_path
            )
            FROM STDIN
            WITH (
                FORMAT csv,
                HEADER TRUE,
                QUOTE '"',
                ESCAPE '"',
                NULL ''
            )
            """,
            file
        )

    conn.commit()
    conn.close()

if __name__ == "__main__":
    load_to_sql()