import csv

from dotenv import load_dotenv
import pandas as pd
import aiohttp
import asyncio
from aiolimiter import AsyncLimiter

from utils import get_api_key, get_root_dir, ensure_path_exists, resolve_write_mode, chunked, fetch_api_data, serialize_json

load_dotenv()

PROJECT_ROOT = get_root_dir()
DATA_DIR = PROJECT_ROOT / "data"
CREDITS_CSV = DATA_DIR / "credits.csv"
PEOPLE_CSV = DATA_DIR / "people.csv"
BATCH_SIZE = 500 # how many ids to process before writing to csv
BASE_URL = "https://api.themoviedb.org/3/person/{person_id}"
API_KEY = get_api_key()

ensure_path_exists(DATA_DIR)
# the credits.csv file needs to exist for this script to work
try:
    df_credits = pd.read_csv(CREDITS_CSV, engine="python")
    df_credits = df_credits[df_credits["order"].astype(int) <= 5] # limit to top actors per film
    all_people_ids = list(set(df_credits["id"]))
except:
    raise FileNotFoundError("You must run get_credits.py before running this script")

# check if people.csv exists and if should overwrite or append
options = resolve_write_mode(PEOPLE_CSV, "id", all_people_ids)
initial_write_mode = options["write_mode"]
ids_to_fetch = options["ids_to_fetch"]
initial_header = options["header"]

params = {
    "api_key": API_KEY
}

# asynchronous session options
limiter = AsyncLimiter(max_rate=30, time_period=1)
semaphore = asyncio.Semaphore(10)


# get cast data
async def collect_people():
    write_mode = "w" if initial_header else "a"
    local_header = initial_header 

    async with aiohttp.ClientSession() as session:
        for i, batch in enumerate(chunked(ids_to_fetch, BATCH_SIZE)):
            tasks = []
            for id in batch:
                url = str.format(BASE_URL, person_id=id)
                tasks.append(fetch_api_data(url, session, params, semaphore, limiter))
            results = await asyncio.gather(*tasks)

            # write batch to csv
            df = pd.DataFrame(results)
            df.to_csv(PEOPLE_CSV, mode = write_mode, index=False, header=local_header,
                    quoting=csv.QUOTE_ALL)
            write_mode = "a"
            local_header=False
        
if __name__ == "__main__":
    asyncio.run(collect_people())