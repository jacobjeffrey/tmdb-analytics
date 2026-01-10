import os
from dotenv import load_dotenv
from pathlib import Path
import json
from typing import Dict, Any, Optional

import aiohttp
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import yaml

try:
    import fsspec
    from google.oauth2 import service_account
    FSSPEC_AVAILABLE = True
except ImportError:
    FSSPEC_AVAILABLE = False

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
    
    Environment variables can override config values:
    - GCS_BUCKET_NAME: Overrides filesystem.gcs.bucket
    """
    package_dir = Path(__file__).resolve().parent
    config_path = package_dir / config_file

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    
    # Allow environment variable overrides
    gcs_bucket = os.getenv("GCS_BUCKET_NAME")
    if gcs_bucket and "filesystem" in cfg and "gcs" in cfg["filesystem"]:
        cfg["filesystem"]["gcs"]["bucket"] = gcs_bucket
    
    return cfg
    
def ensure_dir_exists(path: str | Path, fs=None):
    """
    Ensures a specific directory exists.
    """
    if fs:
        # Cloud/fsspec: treat path as string, use fs.makedirs
        fs.makedirs(str(path), exist_ok=True)
    else:
        # Local: use pathlib
        Path(path).mkdir(parents=True, exist_ok=True)

def ensure_parent_exists(file_path: str | Path, fs=None):
    """
    Ensures the parent directory of a file exists.
    Useful for 'data/movies/file.parquet' -> ensures 'data/movies' exists.
    """
    if fs:
        # Cloud: use string manipulation or Path.parent
        parent = str(Path(file_path).parent)
        fs.makedirs(parent, exist_ok=True)
    else:
        # Local
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

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
async def fetch_api_data(url, session, params, semaphore, limiter, serialize=False, timeout=30):
    """
    Fetch data from API with retry logic.
    
    Args:
        url: API endpoint URL
        session: aiohttp ClientSession
        params: Query parameters dict
        semaphore: asyncio.Semaphore for concurrency control
        limiter: AsyncLimiter for rate limiting
        serialize: Whether to serialize JSON fields
        timeout: Request timeout in seconds (default: 30)
    """
    client_timeout = aiohttp.ClientTimeout(total=timeout)
    
    async with semaphore:
        async with limiter:
            async with session.get(url, params=params, timeout=client_timeout) as response:
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


# ============================================================================
# Filesystem utilities using fsspec
# ============================================================================

def get_filesystem(cfg: Dict[str, Any]):
    """
    Get an fsspec filesystem instance based on config.
    
    Args:
        cfg: Configuration dict with 'filesystem' section
        
    Returns:
        fsspec filesystem instance (local or GCS)
        
    Raises:
        ImportError: If fsspec/gcsfs not installed when using GCS
        ValueError: If backend is unknown
    """
    if not FSSPEC_AVAILABLE:
        raise ImportError(
            "fsspec and/or gcsfs not installed. Install with: pip install fsspec gcsfs"
        )
    
    fs_cfg = cfg.get("filesystem", {})
    backend = fs_cfg.get("backend", "local")
    
    if backend == "local":
        return fsspec.filesystem("file")
    
    elif backend == "gcs":
        gcs_cfg = fs_cfg.get("gcs", {})
        auth_cfg = gcs_cfg.get("auth", {})
        auth_method = auth_cfg.get("method", "adc")
        
        storage_options = {}
        
        if auth_method == "service_account":
            sa_path = auth_cfg.get("service_account_json")
            if sa_path:
                credentials = service_account.Credentials.from_service_account_file(
                    sa_path
                )
                storage_options["token"] = credentials
        
        if gcs_cfg.get("project"):
            storage_options["project"] = gcs_cfg["project"]
        
        return fsspec.filesystem("gcs", **storage_options)
    
    else:
        raise ValueError(f"Unknown filesystem backend: {backend}")


def build_fs_path(cfg: Dict[str, Any], *path_parts: str) -> str:
    """
    Build a filesystem path based on backend configuration.
    
    For local: returns a regular path string (e.g., "data/movies")
    For GCS: returns a gs:// URL (e.g., "gs://bucket-name/tmdb_ingestion/data/movies")
    
    If the first path_part is already a full path (starts with gs:// or is absolute),
    it will be used as the base and remaining parts will be appended.
    
    Args:
        cfg: Configuration dict with 'filesystem' section
        *path_parts: Path components to join
        
    Returns:
        Path string appropriate for the backend
    """
    if not path_parts:
        raise ValueError("At least one path part is required")
    
    fs_cfg = cfg.get("filesystem", {})
    backend = fs_cfg.get("backend", "local")
    
    # Check if first part is already a full path
    first_part = path_parts[0]
    remaining_parts = path_parts[1:]
    
    # For GCS: if first part starts with gs://, use it as base
    if first_part.startswith("gs://"):
        if remaining_parts:
            # Remove gs:// prefix, split, append new parts, rejoin
            path_without_prefix = first_part[5:]  # Remove "gs://"
            existing_parts = path_without_prefix.split("/")
            existing_parts = [p for p in existing_parts if p]  # Remove empty
            existing_parts.extend(remaining_parts)
            existing_parts = [p for p in existing_parts if p]  # Remove empty again
            return f"gs://{'/'.join(existing_parts)}"
        return first_part
    
    # For local: if first part is absolute, use it as base
    if Path(first_part).is_absolute():
        if remaining_parts:
            return str(Path(first_part, *remaining_parts))
        return first_part
    
    # Otherwise, build from scratch
    if backend == "local":
        return str(Path(*path_parts))
    
    elif backend == "gcs":
        gcs_cfg = fs_cfg.get("gcs", {})
        bucket = gcs_cfg.get("bucket")
        prefix = gcs_cfg.get("prefix", "")
        
        if not bucket:
            raise ValueError("GCS bucket not configured in filesystem.gcs.bucket")
        
        parts = [bucket]
        if prefix:
            parts.append(prefix)
        parts.extend(path_parts)
        
        # Remove empty parts and join
        parts = [p for p in parts if p]
        return f"gs://{'/'.join(parts)}"
    
    else:
        raise ValueError(f"Unknown filesystem backend: {backend}")


def get_data_dir_path(cfg: Dict[str, Any]) -> str:
    """
    Get the data directory path string based on backend.
    
    Returns the configured data_dir for local, or "data" for GCS (relative to prefix).
    """
    fs_cfg = cfg.get("filesystem", {})
    backend = fs_cfg.get("backend", "local")
    
    if backend == "local":
        return fs_cfg.get("local", {}).get("data_dir", "data")
    else:
        return "data"


def get_seeds_dir_path(cfg: Dict[str, Any]) -> str:
    """
    Get the seeds directory path string based on backend.
    
    Returns the configured seeds_dir for local, or "dbt/seeds" for GCS (relative to prefix).
    """
    fs_cfg = cfg.get("filesystem", {})
    backend = fs_cfg.get("backend", "local")
    
    if backend == "local":
        return fs_cfg.get("local", {}).get("seeds_dir", "dbt/seeds")
    else:
        return "dbt/seeds"