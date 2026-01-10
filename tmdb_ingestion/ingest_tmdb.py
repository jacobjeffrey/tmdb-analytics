from __future__ import annotations

import argparse
import time
from typing import Dict, Any

from tmdb_ingestion.utils import load_config
from tmdb_ingestion.jobs.discover_movies import run_discover_movies
from tmdb_ingestion.jobs.fetch_movie_details import run_movie_details


def run_full_ingestion(cfg: Dict[str, Any]) -> None:
    """
    Orchestrates the full TMDB ingestion pipeline:
    1. Discover movies by year range
    2. Fetch details and credits for discovered movies
    """
    start = time.time()
    
    print("=" * 60)
    print("Starting TMDB full ingestion pipeline")
    print("=" * 60)
    
    # Step 1: Discover movies
    print("\n[Step 1/2] Discovering movies...")
    try:
        run_discover_movies(cfg)
        print("Movie discovery complete")
    except Exception as e:
        print(f"ERROR: Movie discovery failed: {e}")
        raise
    
    # Step 2: Get details and credits
    print("\n[Step 2/2] Fetching movie details...")
    try:
        run_movie_details(cfg)
        print("Movie details...")
    except Exception as e:
        print(f"ERROR: Movie details fetch failed: {e}")
        raise
    
    # Summary
    end = time.time()
    elapsed_minutes = (end - start) / 60
    
    print("\n" + "=" * 60)
    print(f"TMDB ingestion pipeline complete!")
    print(f"Total elapsed time: {elapsed_minutes:.2f} minutes")
    print("=" * 60)


def _parse_args() -> argparse.Namespace:
    """
    CLI parser for full ingestion orchestration.
    """
    parser = argparse.ArgumentParser(
        description="Run full TMDB ingestion pipeline (discover + details)."
    )
    parser.add_argument(
        "--start-year",
        type=int,
        help="Override start year for discovery",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        help="Override end year for discovery",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Override batch size for details fetch",
    )
    parser.add_argument(
        "--vote-count-gte",
        type=int,
        help="Override minimum vote count filter",
    )
    return parser.parse_args()


if __name__ == "__main__":
    cfg = load_config()
    args = _parse_args()
    
    # Apply CLI overrides
    if args.start_year is not None:
        cfg["discover"]["start_year"] = args.start_year
    if args.end_year is not None:
        cfg["discover"]["end_year"] = args.end_year
    if args.batch_size is not None:
        cfg["ingestion"]["batch_size"] = args.batch_size
    if args.vote_count_gte is not None:
        cfg["discover"]["vote_count_gte"] = args.vote_count_gte
    
    run_full_ingestion(cfg)