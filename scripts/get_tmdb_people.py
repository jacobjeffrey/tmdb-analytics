import os
import time
from pathlib import Path

from dotenv import load_dotenv
import requests
import pandas

load_dotenv()

current_file = Path(__file__).resolve()
project_root = current_file.parents[1]
data_path = project_root / "data" / "people.csv"

api_key = os.getenv("TMDB_API_KEY")
url = "https://api.themoviedb.org/3/person/popular"
session = requests.Session()
people_data = []

# initial call to see how many pages we have
params = {
    "api_key": api_key
}

response = session.get(url, params=params)
data = response.json()
print(data.get("total_pages"))
# # get movie data
# for start, end in year_ranges:
#     print(f"Fetching movies for year starting {start}")
#     params = {
#         "api_key": api_key,
#         "primary_release_date.gte": start,
#         "primary_release_date.lte": end,
#         "vote_count.gte": 10 # filter out low quality and obscure results
#     }

#     for page in range(1, 501):
#         params["page"] = page
#         response = session.get(url, params=params)
#         response.raise_for_status()
#         data = response.json()

#         if not data.get("results"):
#             print(f"Last page was {page-1}")
#             break
#         else:
#             movies = response.json()["results"]
#             movies_data.extend(movies)
#             time.sleep(.05)

#     time.sleep(.50)

# df = pandas.DataFrame(people_data)
# df.to_csv(data_path, index=False)