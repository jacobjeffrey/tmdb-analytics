import io
import csv
import json

import psycopg2
import pandas as pd
import numpy as np

from utils import get_root_dir

PROJECT_ROOT = get_root_dir()
DATA_DIR = PROJECT_ROOT / "data"
MOVIE_DETAILS_FILE = DATA_DIR / "movie_details.parquet"
CREDITS_FILE = DATA_DIR / "credits.parquet"

# database settings
DB_HOST = "localhost"
DB_NAME = "tmdb"
DB_PORT = "5432"
DB_USER = "tmdb"
DB_PASSWORD = "tmdb"



def load_parquet(filepath, table_name, conn):
    print(f"Updating {table_name}")
    df = pd.read_parquet(filepath)

    # sanitize np.ndarrays
    for col in df.select_dtypes(include=['object']).columns:
        # get the first non-null value to check its type
        first_valid = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
        
        # if the column contains NumPy arrays (common in Parquet), convert to Python lists
        if isinstance(first_valid, np.ndarray):
            df[col] = df[col].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)

    # truncate for full refresh
    cur = conn.cursor()
    cur.execute(f"TRUNCATE {table_name}")

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, quoting=csv.QUOTE_ALL)
    buffer.seek(0)

    cur.copy_expert(
    f"""
    COPY {table_name} FROM STDIN
    WITH (FORMAT csv, HEADER TRUE, QUOTE '"', ESCAPE '"', NULL '')
    """,
    buffer
    )
    
    conn.commit()
    print(f"{table_name} updated successfully")

def load_to_sql():
    conn = psycopg2.connect(database=DB_NAME,
                        user=DB_USER,
                        host=DB_HOST,
                        password=DB_PASSWORD,
                        port=DB_PORT)
    # update tables
    load_parquet(MOVIE_DETAILS_FILE, "raw.movie_details", conn)
    load_parquet(CREDITS_FILE, "raw.credits", conn)

    conn.close()

if __name__ == "__main__":
    load_to_sql()