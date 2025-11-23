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

# get database settings
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def load_to_sql():
    # TODO
    pass

if __name__ == "__main__":
    load_to_sql()