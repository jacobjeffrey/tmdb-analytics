import os
import time
from pathlib import Path

from dotenv import load_dotenv
import requests
import pandas

load_dotenv()

current_file = Path(__file__).resolve()
project_root = current_file.parents[1]
data_path = project_root / "data" / "movies.csv"

api_key = os.getenv("TMDB_API_KEY")
url = "https://api.themoviedb.org/3/discover/movie"
session = requests.Session()
movies_data = []

# because TMDB caps pagination at 500, we need to pull data for each year individually
start_year = 2000
end_year = 2024

year_ranges = [(f"{year}-01-01", f"{year}-12-31") for year in range(start_year, end_year+1)]
    
# get movie data
for start, end in year_ranges:
    print(f"Fetching movies for year starting {start}")
    params = {
        "api_key": api_key,
        "primary_release_date.gte": start,
        "primary_release_date.lte": end,
        "vote_count.gte": 10 # filter out low quality and obscure results
    }

    for page in range(1, 501):
        params["page"] = page
        response = session.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if not data.get("results"):
            print(f"Last page was {page-1}")
            break
        else:
            movies = response.json()["results"]
            movies_data.extend(movies)
            time.sleep(.05)

    time.sleep(.50)

df = pandas.DataFrame(movies_data)
df.to_csv(data_path, index=False)