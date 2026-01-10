"""
tmdb_ingestion

A small, config-driven ingestion package for pulling movie data from TMDB.

Layout:

- utils.py
    Shared helpers (config loading, API key, HTTP client, rate limiting, etc.)

- jobs/
    discover_movies.py
    fetch_details_and_credits.py
    update_seeds.py

- ingest_tmdb.py
    Optional orchestrator that can call multiple jobs in sequence.
"""

from .utils import (
    load_config,
    get_api_key,
    fetch_api_data,
    chunked,
    ensure_dir_exists,
    ensure_parent_exists,
)

__all__ = [
    "load_config",
    "get_api_key",
    "fetch_api_data",
    "chunked",
    "ensure_dir_exists",
    "ensure_parent_exists",
]

# Optional: version placeholder if you ever package this
__version__ = "0.1.0"
