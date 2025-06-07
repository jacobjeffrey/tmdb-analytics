import os
import sys

from dotenv import load_dotenv
import requests

load_dotenv()

api_key = os.getenv("TMDB_API_KEY")
url = "https://api.themoviedb.org/3/discover/movie"
params = {
    "api_key": api_key,
    "primary_release_date.gte": "2000-01-01",
}

response = requests.get(url, params=params)
data = response.json()
print(data)