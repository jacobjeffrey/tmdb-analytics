import os
from dotenv import load_dotenv
from pathlib import Path
import json

import pandas as pd
import aiohttp
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

load_dotenv()

def get_api_key():
    try:
        api_key = os.getenv("TMDB_API_KEY")
        return api_key
    except:
        raise FileNotFoundError("TMDB_API_KEY not found in .env file.")
    
def chunked(iterable, batch_size):
    iterable = list(iterable)
    for i in range(0, len(iterable), batch_size):
        yield iterable[i:i+batch_size]


# find root directory by finding the one that contains "requirements.txt"
def get_root_dir(marker="requirements.txt"):
    path = Path(__file__).resolve()
    for parent in path.parents:
        if (parent / marker).exists():
            return parent
    raise RuntimeError(f"Could not find project root (missing '{marker}')")

def ensure_path_exists(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def resolve_write_mode(filepath, id_name, all_ids):
    if filepath.exists():
        filename = filepath.name
        user_response = input(f"{filename} exists, overwrite? (y/n):")
        if user_response.lower() == 'y':
            write_mode = 'w'
            ids_to_fetch = list(all_ids)
            print(f"Fetching {len(ids_to_fetch)} entries")
        else:
            write_mode = 'a'
            ids_seen = pd.read_csv(filepath)[id_name]
            ids_to_fetch = list(set(all_ids) - set(ids_seen))
            print(f"Currently have {len(ids_seen)} entries fetched, will fetch {len(ids_to_fetch)}")
    else:
        write_mode = 'w'
        ids_to_fetch = list(all_ids)
        print(f"Fetching {len(ids_to_fetch)} entries")
        
    header = (write_mode == 'w')

    return {
        "write_mode": write_mode,
        "ids_to_fetch": ids_to_fetch,
        "header": header
    }

def notify_before_retry(retry_state):
    print(f"Retrying movie {retry_state.args[1]}: attempt {retry_state.attempt_number}")


def serialize_json(data):
    return {
        k: json.dumps(v) if isinstance(v, (list, dict)) else v
        for k, v in data.items()
    }

@retry(
    wait=wait_exponential(multiplier=1, min=1, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
    before_sleep = notify_before_retry
)
async def fetch_api_data(url, session, params, semaphore, limiter, serialize=True):
    timeout = aiohttp.ClientTimeout(total=30)  # 30 second total timeout
    
    async with semaphore:
        async with limiter:
            async with session.get(url, params=params, timeout=timeout) as response:
                try:
                    response.raise_for_status()
                    try:
                        data = await response.json()
                        if serialize:
                            return serialize_json(data)
                        else:
                            return data
                    except Exception as err:
                        print(f"Could not fetch data for {url} with params {params}: {err}")
                        return None
                except aiohttp.ClientResponseError as e:
                    print(f"Script failed for {url} with params {params}")
                    return None  # Should probably return None here too