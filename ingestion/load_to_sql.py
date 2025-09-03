import psycopg2
import pandas as pd
from utils import get_root_dir

PROJECT_ROOT = get_root_dir()
DATA_DIR = PROJECT_ROOT / "data"
MOVIES_CSV = DATA_DIR / "movies.csv"
MOVIE_DETAILS_CSV = DATA_DIR / "movie_details.csv"

conn = psycopg2.connect(database="tmdb",
                        user="tmdb",
                        host="localhost",
                        password="tmdb",
                        port=5432)
cur = conn.cursor()

# create raw, stg, mart schemas
cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
cur.execute("CREATE SCHEMA IF NOT EXISTS stg")
cur.execute("CREATE SCHEMA IF NOT EXISTS mart")

# create movies table
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS raw.movies (
    adult TEXT,
    backdrop_path TEXT,
    genre_ids TEXT,
    id TEXT,
    original_language TEXT,
    original_title TEXT,
    overview TEXT,
    popularity TEXT,
    poster_path TEXT,
    release_date TEXT,
    title TEXT,
    video TEXT,
    vote_average TEXT,
    vote_count TEXT
    )
    """)
conn.commit()

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
print("raw.movies created successfully")

# create movie_details
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS raw.movie_details (
    adult TEXT,
    backdrop_path TEXT,
    belongs_to_collection TEXT,
    budget TEXT,
    genres TEXT,
    homepage TEXT,
    id TEXT,
    imdb_id TEXT,
    origin_country TEXT,
    original_language TEXT,
    original_title TEXT,
    overview TEXT,
    popularity TEXT,
    poster_path TEXT,
    production_companies TEXT,
    production_countries TEXT,
    release_date TEXT,
    revenue TEXT,
    runtime TEXT,
    spoken_languages TEXT,
    status TEXT,
    tagline TEXT,
    title TEXT,
    video TEXT,
    vote_average TEXT,
    vote_count TEXT
    )
    """
)
conn.commit()

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
print("raw.movie_details created successfully")

conn.close()