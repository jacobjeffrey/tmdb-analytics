from __future__ import annotations

import argparse
import csv
from typing import Dict, Any

import pandas as pd
import requests

from tmdb_ingestion.utils import (
    load_config,
    get_api_key,
    get_filesystem,
    get_seeds_dir_path,
    build_fs_path,
    ensure_dir_exists,
)


def run_update_seeds(cfg: Dict[str, Any]) -> None:
    """
    Public job entrypoint (sync).
    - Fetches genres, countries, and languages from TMDB
    - Writes them to CSV files using the configured filesystem
    """
    api_key = get_api_key()

    api_cfg = cfg["api"]
    
    # --- Filesystem Setup ---
    fs = get_filesystem(cfg)
    seeds_dir_str = get_seeds_dir_path(cfg)
    
    # Ensure seeds directory exists
    # If GCS, this might be the first time we write to this prefix
    seeds_dir = build_fs_path(cfg, seeds_dir_str)
    ensure_dir_exists(seeds_dir, fs=fs)

    genres_csv = build_fs_path(cfg, seeds_dir, "genres.csv")
    countries_csv = build_fs_path(cfg, seeds_dir, "countries.csv")
    languages_csv = build_fs_path(cfg, seeds_dir, "languages.csv")

    session = requests.Session()
    params = {"api_key": api_key}

    genres_url = api_cfg["genres_url"]
    countries_url = api_cfg["countries_url"]
    languages_url = api_cfg["languages_url"]

    print(f"Updating seed files in {seeds_dir}")

    # Helper to write dataframe to fs
    def write_csv(df: pd.DataFrame, path: str, name: str):
        # fs.open() creates a file-like object (works for Local or GCS)
        with fs.open(path, "w") as f:
            df.to_csv(f, index=False, quoting=csv.QUOTE_ALL)
        print(f"Wrote {name} to {path}")

    # Genres
    genres_response = session.get(genres_url, params=params)
    genres_response.raise_for_status()
    genres_data = genres_response.json().get("genres", [])
    write_csv(pd.DataFrame(genres_data), genres_csv, "genres")

    # Countries
    countries_response = session.get(countries_url, params=params)
    countries_response.raise_for_status()
    countries_data = countries_response.json()
    write_csv(pd.DataFrame(countries_data), countries_csv, "countries")

    # Languages
    languages_response = session.get(languages_url, params=params)
    languages_response.raise_for_status()
    languages_data = languages_response.json()
    write_csv(pd.DataFrame(languages_data), languages_csv, "languages")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Update TMDB seed CSVs (genres, countries, languages)."
    )
    return parser.parse_args()


if __name__ == "__main__":
    cfg = load_config()
    _ = _parse_args()
    run_update_seeds(cfg)