from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Dict, Any, List

import aiohttp
import pandas as pd
from aiolimiter import AsyncLimiter
from tqdm.asyncio import tqdm_asyncio

from tmdb_ingestion.utils import (
    load_config,
    get_api_key,
    ensure_path_exists,
    fetch_api_data,
)


def run_discover_movies(cfg: Dict[str, Any]) -> None:
    """
    Public job entrypoint (sync).
    - Reads the relevant config values
    - Sets up paths
    - Calls the async worker
    """
    api_key = get_api_key()

    api_cfg = cfg["api"]             # e.g. base URL, endpoints
    discover_cfg = cfg["discover"]   # start_year, end_year, vote_count_gte, filters
    conc_cfg = cfg["concurrency"]    # rate limit / semaphore limits
    paths_cfg = cfg["paths"]         # data_dir, etc.
    storage_cfg = cfg.get("storage", {})  # compression, engine settings

    start_year = discover_cfg["start_year"]
    end_year = discover_cfg["end_year"]
    vote_count_gte = discover_cfg.get("vote_count_gte", 100)  # Default to 100 for quality

    data_root = Path(paths_cfg["data_dir"])
    movies_dir = data_root / "movies"
    ensure_path_exists(movies_dir)

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
        )
    )


async def _discover_movies_for_range(
    start_year: int,
    end_year: int,
    api_key: str,
    api_cfg: Dict[str, Any],
    discover_cfg: Dict[str, Any],
    conc_cfg: Dict[str, Any],
    movies_dir: Path,
    vote_count_gte: int = 100,  # Required with sensible default
    storage_cfg: Dict[str, Any] = None,
) -> None:
    """
    Actual async ingestion logic.
    - Loops over years
    - Calls TMDB discover endpoint with pagination
    - Writes one Parquet file per year
    - Uses date-based filtering and sorting to avoid pagination quirks
    """
    if storage_cfg is None:
        storage_cfg = {}
    
    base_url = api_cfg["discover_url"]  # e.g. https://api.themoviedb.org/3/discover/movie

    limiter = AsyncLimiter(conc_cfg["max_rate"], conc_cfg["time_period"])
    semaphore = asyncio.Semaphore(conc_cfg["semaphore_limit"])

    # Get discover config values
    include_adult = discover_cfg.get("include_adult", False)
    sort_by = discover_cfg.get("sort_by", "primary_release_date.asc")
    max_pages = discover_cfg.get("max_pages", 500)
    
    # Get storage config values
    compression = storage_cfg.get("compression", "snappy")
    engine = storage_cfg.get("engine", "pyarrow")
    
    # Get API timeout
    timeout = api_cfg.get("timeout", 30)

    async with aiohttp.ClientSession() as session:
        for year in range(start_year, end_year + 1):
            print(f"Discovering movies for year {year}...")

            movies: List[Dict[str, Any]] = []

            # Base params for *all* pages for this year
            # Use date ranges instead of year for more precise filtering
            params = {
                "api_key": api_key,
                "primary_release_date.gte": f"{year}-01-01",
                "primary_release_date.lte": f"{year}-12-31",
                "include_adult": str(include_adult).lower(),
                "vote_count.gte": vote_count_gte,  # Filter out low quality/obscure results
                "sort_by": sort_by,  # Sort to avoid pagination quirks
                "page": 1,
            }

            # --- First page ---
            first_page = await fetch_api_data(
                url=base_url,
                session=session,
                params=params,
                semaphore=semaphore,
                limiter=limiter,
                timeout=timeout,
            )

            if not first_page:
                print(f"No data returned for year {year}, skipping.")
                continue

            # Defensive: check for results in first page
            first_page_results = first_page.get("results")
            if not first_page_results:
                print(f"No movies found in first page for year {year}, skipping.")
                continue

            movies.extend(first_page_results)

            total_pages = first_page.get("total_pages", 1)

            # TMDB safeguard: Discover endpoint cannot go beyond max_pages
            if total_pages > max_pages:
                print(
                    f"WARNING: TMDB returned {total_pages} pages for year {year}, "
                    f"but the API only allows access to the first {max_pages} pages. "
                    f"Consider increasing vote_count.gte (currently {vote_count_gte}) "
                    f"to reduce result set."
                )
                total_pages = max_pages  # clamp to TMDB's maximum

            # --- Remaining pages with progress bar ---
            if total_pages > 1:
                page_tasks = []
                for page in range(2, total_pages + 1):
                    page_params = dict(params, page=page)
                    task = fetch_api_data(
                        url=base_url,
                        session=session,
                        params=page_params,
                        semaphore=semaphore,
                        limiter=limiter,
                        timeout=timeout,
                    )
                    page_tasks.append(task)

                # Show progress bar, only keep last year's bar visible
                leave_bar = (year == end_year)
                page_results = await tqdm_asyncio.gather(
                    *page_tasks,
                    total=len(page_tasks),
                    desc=f"Downloading pages for year {year}",
                    leave=leave_bar,
                )

                # Defensive: check each page for results
                for page_data in page_results:
                    if not page_data:
                        continue
                    results = page_data.get("results")
                    if results:
                        movies.extend(results)

            if not movies:
                print(f"No movies found for year {year}, skipping write.")
                continue

            df = pd.DataFrame(movies)
            out_path = movies_dir / f"movies_{year}.parquet"
            ensure_path_exists(out_path)
            df.to_parquet(out_path, index=False, engine=engine, compression=compression)
            print(
                f"Wrote {len(df)} movies for {year} "
                f"(vote_count.gte={vote_count_gte}) to {out_path}"
            )


def _parse_args() -> argparse.Namespace:
    """
    CLI parser for this job.
    Allows overriding start_year / end_year from the command line.
    """
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