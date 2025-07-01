import os
import time
import json
from pathlib import Path

from dotenv import load_dotenv
import requests
import pandas

load_dotenv()

current_file = Path(__file__).resolve()
project_root = current_file.parents[1]
data_path = project_root / "data" / "movies.csv"
os.makedirs(os.path.dirname(data_path), exist_ok=True) # ensure path exists

api_key = os.getenv("TMDB_API_KEY")
url = "https://api.themoviedb.org/3/discover/movie"
session = requests.Session()
movies_data = []

# because TMDB caps pagination at 500, we need to pull data for each year individually
start_year = 2000
end_year = 2024

year_ranges = [(f"{year}-01-01", f"{year}-12-31") for year in range(start_year, end_year+1)]
    
# this formula removes any unusual unicode seperators
def clean_text(text):
    text = text.replace(u"\u2028", " ")
    return text.replace(u"\u2029", " ")

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

        try:
            data = response.json()
        except ValueError:
            print(f"Invalid JSON on page {page} for range {start} to {end}")
            break
        
        if not data.get("results"):
            break
        
        movies = data["results"]
        # clean the data
        for movie in movies:
            for key, value in movie.items(): 
                if (isinstance(value,(list, dict))): # deserialize lists
                    movie[key] = json.dumps(value)
                elif (isinstance(value, str)):
                    movie[key] = clean_text(value)

        movies_data.extend(movies)
        time.sleep(.05)

    time.sleep(.10)

df = pandas.DataFrame(movies_data)
df.to_csv(data_path, index=False)