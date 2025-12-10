from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Dict, Any, List

import aiohttp
import pandas as pd
from aiolimiter import AsyncLimiter

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
    ingest_cfg = cfg["ingestion"]    # start_year, end_year, batch_size, vote_count_gte
    conc_cfg = cfg["concurrency"]    # rate limit / semaphore limits
    paths_cfg = cfg["paths"]         # data_dir, etc.

    start_year = ingest_cfg["start_year"]
    end_year = ingest_cfg["end_year"]
    vote_count_gte = ingest_cfg.get("vote_count_gte")  # <-- NEW: pull from config

    data_root = Path(paths_cfg["data_dir"])
    movies_dir = data_root / "movies"
    ensure_path_exists(movies_dir)

    asyncio.run(
        _discover_movies_for_range(
            start_year=start_year,
            end_year=end_year,
            api_key=api_key,
            api_cfg=api_cfg,
            conc_cfg=conc_cfg,
            movies_dir=movies_dir,
            vote_count_gte=vote_count_gte,  # <-- NEW: pass through
        )
    )


async def _discover_movies_for_range(
    start_year: int,
    end_year: int,
    api_key: str,
    api_cfg: Dict[str, Any],
    conc_cfg: Dict[str, Any],
    movies_dir: Path,
    vote_count_gte: int | None = None,  # <-- NEW: parameter
) -> None:
    """
    Actual async ingestion logic.
    - Loops over years
    - Calls TMDB discover endpoint with pagination
    - Writes one Parquet file per year
    """
    base_url = api_cfg["discover_url"]  # e.g. https://api.themoviedb.org/3/discover/movie

    limiter = AsyncLimiter(conc_cfg["max_rate"], conc_cfg["time_period"])
    semaphore = asyncio.Semaphore(conc_cfg["semaphore_limit"])

    async with aiohttp.ClientSession() as session:
        for year in range(start_year, end_year + 1):
            print(f"📆 Discovering movies for year {year}...")

            movies: List[Dict[str, Any]] = []

            # Base params for *all* pages for this year
            params = {
                "api_key": api_key,
                "primary_release_year": year,
                "include_adult": "false",  # NOTE: must be string, not bool
                "page": 1,
            }

            # 🔥 NEW: apply vote_count_gte if configured
            if vote_count_gte is not None:
                params["vote_count.gte"] = vote_count_gte

            # --- First page ---
            first_page = await fetch_api_data(
                url=base_url,
                session=session,
                params=params,
                semaphore=semaphore,
                limiter=limiter,
            )

            if not first_page:
                print(f"⚠️ No data returned for year {year}, skipping.")
                continue

            total_pages = first_page.get("total_pages", 1)

            # 🔥 TMDB safeguard: Discover endpoint cannot go beyond 500 pages
            if total_pages > 500:
                print(
                    f"⚠️ WARNING: TMDB returned {total_pages} pages for year {year}, "
                    f"but the API only allows access to the first 500 pages. "
                    f"Results beyond page 500 will NOT be fetched."
                )
                total_pages = 500  # clamp to TMDB's maximum
            movies.extend(first_page.get("results", []))

            # --- Remaining pages ---
            if total_pages > 1:
                page_tasks = []
                for page in range(2, total_pages + 1):
                    page_params = dict(params, page=page)  # inherits vote_count.gte, etc.
                    task = fetch_api_data(
                        url=base_url,
                        session=session,
                        params=page_params,
                        semaphore=semaphore,
                        limiter=limiter,
                    )
                    page_tasks.append(task)

                page_results = await asyncio.gather(*page_tasks)
                for page_data in page_results:
                    if page_data and "results" in page_data:
                        movies.extend(page_data["results"])

            if not movies:
                print(f"⚠️ No movies found for year {year}, skipping write.")
                continue

            df = pd.DataFrame(movies)
            out_path = movies_dir / f"movies_{year}.parquet"
            ensure_path_exists(out_path)
            df.to_parquet(out_path, index=False)
            print(
                f"✅ Wrote {len(df)} movies for {year} "
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
    return parser.parse_args()


if __name__ == "__main__":
    cfg = load_config()
    args = _parse_args()

    if args.start_year is not None:
        cfg["ingestion"]["start_year"] = args.start_year
    if args.end_year is not None:
        cfg["ingestion"]["end_year"] = args.end_year

    run_discover_movies(cfg)
