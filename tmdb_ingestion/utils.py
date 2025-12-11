import os
from dotenv import load_dotenv
from pathlib import Path
import json

import aiohttp
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import yaml

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

def load_config(config_file="config.yml"):
    """
    Load config.yml from the tmdb_ingestion package directory.
    """
    package_dir = Path(__file__).resolve().parent
    config_path = package_dir / config_file

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)
    
def ensure_path_exists(path: str | Path):
    path = Path(path)
    (path.parent if path.suffix else path).mkdir(parents=True, exist_ok=True)

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
async def fetch_api_data(url, session, params, semaphore, limiter, serialize=False):
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
                    return None 