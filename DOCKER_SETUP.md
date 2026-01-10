# Docker Setup for TMDB Analytics

This guide provides detailed Docker setup instructions for the TMDB Analytics project. For a quick start, see the [README.md](README.md) which uses the recommended Makefile workflow. The default ingestion configuration is cloud-first (GCS + BigQuery); you can switch to local storage for local development.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/) (included with Docker Desktop)
- A TMDB API key ([get one here](https://www.themoviedb.org/settings/api))

## Recommended: Using Makefile (Quick Start, Cloud-First)

The easiest way to get started is using the Makefile, which wraps all Docker commands:

```bash
# Clone and setup
git clone https://github.com/jacobjeffrey/tmdb-analytics.git
cd tmdb-analytics
cp .env.example .env
# Edit .env with your TMDB API key and GCP settings

# First-time setup
make init      # Builds container and starts services

# Run the complete pipeline (BigQuery)
make pipeline  # Runs ingestion + dbt transformations

# View documentation
make dbt-docs  # Available at http://localhost:8080

# See all available commands
make help
```

See the [README.md](README.md) for the full quick start guide.

---

## Alternative: Direct Docker Compose Commands

If you prefer using `docker compose` directly instead of the Makefile:

**1. Clone the repository**
```bash
git clone https://github.com/jacobjeffrey/tmdb-analytics.git
cd tmdb-analytics
```

**2. Set up your API key**
```bash
cp .env.example .env
# Edit .env and add your TMDB API key
```

**3. Build and run the container**
```bash
docker compose up -d
```

**4. Enter the container**
```bash
docker compose exec tmdb-analytics bash
```

**5. Run the pipeline**
```bash
# Inside the container:

# Run ingestion (takes ~10 minutes)
python -m tmdb_ingestion.ingest_tmdb

# You can also run individual ingestion jobs:
python -m tmdb_ingestion.jobs.discover_movies
python -m tmdb_ingestion.jobs.fetch_movie_details
python -m tmdb_ingestion.jobs.update_seeds

# Transform with dbt
cd dbt
dbt deps
dbt seed
dbt run
dbt test

# View documentation
dbt docs generate
dbt docs serve --host 0.0.0.0
```

**Optional: confirm ingestion uses GCS (default)**

By default, ingestion writes to GCS. To verify or update, edit `tmdb_ingestion/config.yml`:
```yaml
filesystem:
  backend: "gcs"
  gcs:
    bucket: "your-bucket-name"
    prefix: "tmdb_ingestion"
    auth:
      method: "adc"  # or "service_account" for production
```
When using `auth.method: "adc"`, authenticate locally with:
```bash
gcloud auth application-default login
```
To switch to local storage, set `filesystem.backend: "local"` and keep the `local` paths below.

**Local Development (DuckDB)**

For local development, switch the filesystem backend and run dbt against DuckDB:
```yaml
# tmdb_ingestion/config.yml
filesystem:
  backend: "local"
  local:
    data_dir: "data"
    seeds_dir: "dbt/seeds"
```

```bash
export DBT_TARGET=dev_duckdb
make pipeline
```

**6. View dbt docs in your browser**

Open http://localhost:8080 to see the dbt documentation.

## Common Docker Compose Commands

```bash
# Start the container
docker compose up -d

# Stop the container
docker compose down

# View logs
docker compose logs -f

# Enter the container shell
docker compose exec tmdb-analytics bash

# Rebuild after code changes
docker compose build

# Run a one-off command
docker compose run --rm tmdb-analytics python -m tmdb_ingestion.jobs.discover_movies
```

## Data Persistence

The `data/` directory is mounted as a volume, so your data persists even when you stop/remove containers:
- `data/*.parquet` - Raw API data
- `data/tmdb_analytics.db` - DuckDB database
- `dbt/seeds/*.csv` - Reference data

## Development Workflow

The Docker setup mounts your local code directories, so you can:
1. Edit code on your host machine
2. Run changes immediately inside the container (no rebuild needed)

```bash
# In one terminal: keep the container running
docker compose up

# In another terminal: make changes, then test
docker compose exec tmdb-analytics python -m tmdb_ingestion.jobs.discover_movies --start-year 2026 --end-year 2026

```

## Troubleshooting

**Container won't start:**
```bash
# Check logs
docker compose logs

# Rebuild from scratch
docker compose down
docker compose build --no-cache
docker compose up
```

**Permission issues with data files:**
```bash
# On Linux, you may need to fix permissions
sudo chown -R $USER:$USER data/
```

**Port 8080 already in use:**
The dbt docs server runs inside the container. If you need to change the port, you can:
- Use make dbt-docs (recommended) which runs the docs server bound to 0.0.0.0. The port mapping is defined in docker-compose.yml
- Or manually expose a different port when running `dbt docs serve` inside the container

## Alternative: Using Dockerfile Only

If you prefer not to use Docker Compose:

```bash
# Build the image
docker build -f Dockerfile.dev -t tmdb-analytics:dev .

# Run the container
docker run -it \
  -v $(pwd)/data:/app/data \
  -e TMDB_API_KEY=your_api_key_here \
  -p 8080:8080 \
  tmdb-analytics
```

## Cleaning Up

```bash
# Stop and remove containers
docker compose down

# Remove containers and volumes
docker compose down -v

# Remove the Docker image
docker rmi tmdb-analytics
```
