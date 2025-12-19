from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List

import aiohttp
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from aiolimiter import AsyncLimiter
from tqdm.asyncio import tqdm_asyncio

from tmdb_ingestion.utils import (
    load_config,
    get_api_key,
    ensure_path_exists,
    fetch_api_data,
)


def run_movie_details(cfg: Dict[str, Any]) -> None:
    """
    Public job entrypoint (sync).
    - Reads config values
    - Sets up paths
    - Calls the async worker
    """
    api_key = get_api_key()

    api_cfg = cfg["api"]
    ingest_cfg = cfg["ingestion"]
    conc_cfg = cfg["concurrency"]
    paths_cfg = cfg["paths"]
    storage_cfg = cfg.get("storage", {})

    batch_size = ingest_cfg.get("batch_size", 500)

    data_root = Path(paths_cfg["data_dir"])
    movies_dir = data_root / "movies"
    details_dir = data_root / "movie_details"
    ensure_path_exists(details_dir)

    # Input: read from partitioned movies files
    movies_files = sorted(movies_dir.glob("movies_*.parquet"))
    if not movies_files:
        raise FileNotFoundError(
            f"No movies_*.parquet files found in {movies_dir}. "
            f"Run discover_movies.py first."
        )

    asyncio.run(
        _fetch_details_and_credits(
            movies_files=movies_files,
            api_key=api_key,
            api_cfg=api_cfg,
            conc_cfg=conc_cfg,
            details_dir=details_dir,
            batch_size=batch_size,
            storage_cfg=storage_cfg,
        )
    )


async def _fetch_details_and_credits(
    movies_files: List[Path],
    api_key: str,
    api_cfg: Dict[str, Any],
    conc_cfg: Dict[str, Any],
    details_dir: Path,
    batch_size: int = 500,
    storage_cfg: Dict[str, Any] = None,
) -> None:
    """
    Fetch movie details and credits for all discovered movies.
    - Reads from partitioned movies files
    - Fetches in batches with streaming writes
    - Writes to single Parquet file with append mode
    """
    if storage_cfg is None:
        storage_cfg = {}
    
    base_url = api_cfg["details_url"]  # e.g. https://api.themoviedb.org/3/movie/
    append_to_response = api_cfg.get("append_to_response", "credits")
    timeout = api_cfg.get("timeout", 30)
    compression = storage_cfg.get("compression", "snappy")

    params = {
        "api_key": api_key,
        "append_to_response": append_to_response,  # Include cast/crew in response
    }

    limiter = AsyncLimiter(conc_cfg["max_rate"], conc_cfg["time_period"])
    semaphore = asyncio.Semaphore(conc_cfg["semaphore_limit"])

    # Load all movie IDs from partitioned files
    print("Loading movie IDs from partitioned files...")
    all_movie_ids = []
    for movie_file in movies_files:
        df = pd.read_parquet(movie_file, columns=["id"])
        all_movie_ids.extend(df["id"].tolist())

    print(f"Found {len(all_movie_ids)} total movies across {len(movies_files)} files")

    output_file = details_dir / "movie_details.parquet"

    # Remove old output file if it exists (fresh start)
    if output_file.exists():
        output_file.unlink()
        print(f"Removed existing output file: {output_file}")

    writer = None  # PyArrow ParquetWriter, created on first batch

    async with aiohttp.ClientSession() as session:
        num_movies = len(all_movie_ids)
        num_batches = (num_movies + batch_size - 1) // batch_size

        print(
            f"Fetching movie details and credits in {num_batches} batches "
            f"(batch_size={batch_size})"
        )

        for batch_idx, batch_start in enumerate(range(0, num_movies, batch_size), start=1):
            batch_ids = all_movie_ids[batch_start : batch_start + batch_size]

            # Create tasks for this batch
            tasks = []
            for movie_id in batch_ids:
                url = f"{base_url}{movie_id}"
                task = _fetch_with_metadata(
                    url=url,
                    session=session,
                    params=params,
                    semaphore=semaphore,
                    limiter=limiter,
                    movie_id=movie_id,
                    timeout=timeout,
                )
                tasks.append(task)

            # Fetch batch with progress indication
            batch_results = await tqdm_asyncio.gather(
                *tasks,
                total=len(tasks),
                desc=f"Batch {batch_idx}/{num_batches}",
                leave=False,
            )

            # Convert to DataFrame
            df_batch = pd.DataFrame(batch_results)

            # Streaming write via PyArrow
            table = pa.Table.from_pandas(df_batch)

            if writer is None:
                writer = pq.ParquetWriter(
                    output_file,
                    table.schema,
                    compression=compression,
                )

            writer.write_table(table)

            print(f"Batch {batch_idx}/{num_batches} complete ({len(df_batch)} movies)")

    # Close writer after all batches
    if writer is not None:
        writer.close()
        print(f"Wrote all movie details to {output_file}")


async def _fetch_with_metadata(
    url: str,
    session: aiohttp.ClientSession,
    params: Dict[str, Any],
    semaphore: asyncio.Semaphore,
    limiter: AsyncLimiter,
    movie_id: int,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    Fetch a single movie's details/credits and wrap with metadata.
    """
    data = await fetch_api_data(
        url=url,
        session=session,
        params=params,
        semaphore=semaphore,
        limiter=limiter,
        timeout=timeout,
    )

    return {
        "movie_id": movie_id,
        "payload_json": data,
        "ingested_at": datetime.now(timezone.utc),
    }


def _parse_args() -> argparse.Namespace:
    """
    CLI parser for this job.
    """
    parser = argparse.ArgumentParser(
        description="Fetch TMDB movie details and credits for discovered movies."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Override batch size for fetching (default: 500)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    cfg = load_config()
    args = _parse_args()

    if args.batch_size is not None:
        cfg["ingestion"]["batch_size"] = args.batch_size

    run_movie_details(cfg)