from __future__ import annotations

import argparse
import asyncio
from typing import Dict, Any, List

import aiohttp
import pandas as pd
from aiolimiter import AsyncLimiter
from tqdm.asyncio import tqdm_asyncio

from tmdb_ingestion.utils import (
    load_config,
    get_api_key,
    fetch_api_data,
    get_filesystem,
    get_data_dir_path,
    build_fs_path,
    ensure_dir_exists,
)


def run_discover_movies(cfg: Dict[str, Any]) -> None:
    """
    Public job entrypoint (sync).
    - Reads the relevant config values
    - Sets up filesystems and paths
    - Calls the async worker
    """
    api_key = get_api_key()

    api_cfg = cfg["api"]             
    discover_cfg = cfg["discover"]   
    conc_cfg = cfg["concurrency"]    
    storage_cfg = cfg.get("storage", {})  

    start_year = discover_cfg["start_year"]
    end_year = discover_cfg["end_year"]
    vote_count_gte = discover_cfg.get("vote_count_gte", 100)

    # --- Filesystem Setup ---
    fs = get_filesystem(cfg)
    data_dir_str = get_data_dir_path(cfg)
    
    # Construct "movies" directory path (works for local or GCS)
    movies_dir = build_fs_path(cfg, data_dir_str, "movies")
    ensure_dir_exists(movies_dir, fs=fs)

    asyncio.run(
        _discover_movies_for_range(
            start_year=start_year,
            end_year=end_year,
            api_key=api_key,
            api_cfg=api_cfg,
            discover_cfg=discover_cfg,
            conc_cfg=conc_cfg,
            movies_dir=movies_dir,
            vote_count_gte=vote_count_gte,
            storage_cfg=storage_cfg,
            fs=fs,  # Pass filesystem object
            cfg=cfg # Pass full cfg for path building
        )
    )


async def _discover_movies_for_range(
    start_year: int,
    end_year: int,
    api_key: str,
    api_cfg: Dict[str, Any],
    discover_cfg: Dict[str, Any],
    conc_cfg: Dict[str, Any],
    movies_dir: str,
    fs: Any,
    cfg: Dict[str, Any],
    vote_count_gte: int = 100,
    storage_cfg: Dict[str, Any] = None,
) -> None:
    """
    Actual async ingestion logic.
    """
    if storage_cfg is None:
        storage_cfg = {}
    
    base_url = api_cfg["discover_url"]

    limiter = AsyncLimiter(conc_cfg["max_rate"], conc_cfg["time_period"])
    semaphore = asyncio.Semaphore(conc_cfg["semaphore_limit"])

    include_adult = discover_cfg.get("include_adult", False)
    sort_by = discover_cfg.get("sort_by", "primary_release_date.asc")
    max_pages = discover_cfg.get("max_pages", 500)
    
    compression = storage_cfg.get("compression", "snappy")
    engine = storage_cfg.get("engine", "pyarrow")
    timeout = api_cfg.get("timeout", 30)

    async with aiohttp.ClientSession() as session:
        for year in range(start_year, end_year + 1):
            print(f"Discovering movies for year {year}...")

            movies: List[Dict[str, Any]] = []

            params = {
                "api_key": api_key,
                "primary_release_date.gte": f"{year}-01-01",
                "primary_release_date.lte": f"{year}-12-31",
                "include_adult": str(include_adult).lower(),
                "vote_count.gte": vote_count_gte,
                "sort_by": sort_by,
                "page": 1,
            }

            # --- Fetching Logic (Same as before) ---
            first_page = await fetch_api_data(
                url=base_url, session=session, params=params,
                semaphore=semaphore, limiter=limiter, timeout=timeout,
            )

            if not first_page:
                print(f"No data returned for year {year}, skipping.")
                continue

            first_page_results = first_page.get("results")
            if not first_page_results:
                print(f"No movies found in first page for year {year}, skipping.")
                continue

            movies.extend(first_page_results)
            total_pages = first_page.get("total_pages", 1)

            if total_pages > max_pages:
                print(
                    f"WARNING: TMDB returned {total_pages} pages for year {year}, "
                    f"clamping to {max_pages}."
                )
                total_pages = max_pages

            if total_pages > 1:
                page_tasks = []
                for page in range(2, total_pages + 1):
                    page_params = dict(params, page=page)
                    task = fetch_api_data(
                        url=base_url, session=session, params=page_params,
                        semaphore=semaphore, limiter=limiter, timeout=timeout,
                    )
                    page_tasks.append(task)

                leave_bar = (year == end_year)
                page_results = await tqdm_asyncio.gather(
                    *page_tasks,
                    total=len(page_tasks),
                    desc=f"Downloading pages for year {year}",
                    leave=leave_bar,
                )

                for page_data in page_results:
                    if page_data and page_data.get("results"):
                        movies.extend(page_data.get("results"))

            if not movies:
                print(f"No movies found for year {year}, skipping write.")
                continue

            # --- Write to Storage ---
            df = pd.DataFrame(movies)
            
            # Use build_fs_path to get correct Local or GCS string
            # We don't join directly on movies_dir in case it's a "gs://" string (pathlib would break)
            out_path = build_fs_path(cfg, movies_dir, f"movies_{year}.parquet")
            
            # Pass filesystem=fs so PyArrow knows how to handle the URL
            df.to_parquet(out_path, index=False, engine=engine, compression=compression, filesystem=fs)
            
            print(
                f"Wrote {len(df)} movies for {year} "
                f"(vote_count.gte={vote_count_gte}) to {out_path}"
            )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Discover TMDB movies for a year range.")
    parser.add_argument("--start-year", type=int, help="Override start year (inclusive)")
    parser.add_argument("--end-year", type=int, help="Override end year (inclusive)")
    parser.add_argument("--vote-count-gte", type=int, help="Override minimum vote count filter")
    return parser.parse_args()


if __name__ == "__main__":
    cfg = load_config()
    args = _parse_args()

    if args.start_year is not None:
        cfg["discover"]["start_year"] = args.start_year
    if args.end_year is not None:
        cfg["discover"]["end_year"] = args.end_year
    if args.vote_count_gte is not None:
        cfg["discover"]["vote_count_gte"] = args.vote_count_gte

    run_discover_movies(cfg)