import csv

from dotenv import load_dotenv
import pandas as pd
import aiohttp
import asyncio
from aiolimiter import AsyncLimiter

from utils import (
    get_api_key,
    get_root_dir,
    ensure_path_exists,
    resolve_write_mode,
    chunked,
    fetch_api_data,
    serialize_json,
)

load_dotenv()

PROJECT_ROOT = get_root_dir()
DATA_DIR = PROJECT_ROOT / "data"
CREDITS_CSV = DATA_DIR / "credits.csv"
PEOPLE_CSV = DATA_DIR / "people.csv"
BATCH_SIZE = 500  # how many ids to process before writing to csv
BASE_URL = "https://api.themoviedb.org/3/person/{person_id}"
API_KEY = get_api_key()

params = {
    "api_key": API_KEY,
}

# asynchronous session options
limiter = AsyncLimiter(max_rate=40, time_period=1)
semaphore = asyncio.Semaphore(10)


async def collect_people():
    ensure_path_exists(DATA_DIR)

    # the credits.csv file needs to exist for this script to work
    try:
        df_credits = pd.read_csv(CREDITS_CSV, engine="python")
        # unique person IDs
        all_people_ids = list(set(df_credits["id"]))
    except FileNotFoundError:
        raise FileNotFoundError("You must run get_credits.py before running this script")
    except Exception as e:
        raise RuntimeError(f"Error reading {CREDITS_CSV}: {e}")

    # check if people.csv exists and if we should overwrite or append
    options = resolve_write_mode(PEOPLE_CSV, "id", all_people_ids)
    write_mode = options["write_mode"]
    ids_to_fetch = options["ids_to_fetch"]
    local_header = options["header"]

    if not len(ids_to_fetch):
        print("No new people IDs to fetch.")
        return

    async with aiohttp.ClientSession() as session:
        for batch in chunked(ids_to_fetch, BATCH_SIZE):
            tasks = []
            for person_id in batch:
                url = BASE_URL.format(person_id=person_id)
                tasks.append(
                    fetch_api_data(
                        url,
                        session,
                        params,
                        semaphore,
                        limiter,
                    )
                )

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Filter out None and exceptions
            valid_results = []
            for r in results:
                if isinstance(r, Exception):
                    print(f"Task failed with exception: {r}")
                elif r is not None:
                    valid_results.append(r)

            if not valid_results:
                continue

            df = pd.DataFrame(valid_results)
            df.to_csv(
                PEOPLE_CSV,
                mode=write_mode,
                index=False,
                header=local_header,
                quoting=csv.QUOTE_ALL,
            )

            write_mode = "a"
            local_header = False


if __name__ == "__main__":
    asyncio.run(collect_people())
