from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, Any

import pandas as pd
import requests

from tmdb_ingestion.utils import (
    load_config,
    get_api_key,
    ensure_path_exists,
)


def run_update_seeds(cfg: Dict[str, Any]) -> None:
    """
    Public job entrypoint (sync).

    - Reads TMDB API key
    - Uses paths from config to determine seeds directory
    - Fetches genres, countries, and languages from TMDB
    - Writes them to CSV files in the seeds directory
    """
    api_key = get_api_key()

    api_cfg = cfg["api"]
    paths_cfg = cfg["paths"]
    seeds_dir = Path(paths_cfg["seeds_dir"])
    ensure_path_exists(seeds_dir)

    genres_csv = seeds_dir / "genres.csv"
    countries_csv = seeds_dir / "countries.csv"
    languages_csv = seeds_dir / "languages.csv"

    session = requests.Session()
    params = {"api_key": api_key}

    genres_url = api_cfg["genres_url"]
    countries_url = api_cfg["countries_url"]
    languages_url = api_cfg["languages_url"]

    print(f"Updating seed files in {seeds_dir.resolve()}")

    # Genres
    genres_response = session.get(genres_url, params=params)
    genres_response.raise_for_status()
    genres_data = genres_response.json().get("genres", [])
    df = pd.DataFrame(genres_data)
    df.to_csv(genres_csv, index=False, quoting=csv.QUOTE_ALL)
    print(f"Wrote genres to {genres_csv}")

    # Countries
    countries_response = session.get(countries_url, params=params)
    countries_response.raise_for_status()
    countries_data = countries_response.json()
    df = pd.DataFrame(countries_data)
    df.to_csv(countries_csv, index=False, quoting=csv.QUOTE_ALL)
    print(f"Wrote countries to {countries_csv}")

    # Languages
    languages_response = session.get(languages_url, params=params)
    languages_response.raise_for_status()
    languages_data = languages_response.json()
    df = pd.DataFrame(languages_data)
    df.to_csv(languages_csv, index=False, quoting=csv.QUOTE_ALL)
    print(f"Wrote languages to {languages_csv}")


def _parse_args() -> argparse.Namespace:
    """
    CLI parser for this job.
    Mostly here for symmetry with other jobs; you can add flags later if needed.
    """
    parser = argparse.ArgumentParser(
        description="Update TMDB seed CSVs (genres, countries, languages)."
    )
    # e.g. you could add --seeds-dir override in the future.
    return parser.parse_args()


if __name__ == "__main__":
    cfg = load_config()
    _ = _parse_args()  # currently unused, but keeps the pattern consistent
    run_update_seeds(cfg)
