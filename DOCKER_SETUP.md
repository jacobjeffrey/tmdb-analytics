# Docker Setup for TMDB Analytics

This guide shows you how to run the TMDB Analytics project using Docker, which simplifies setup by handling all dependencies automatically.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/) (included with Docker Desktop)
- A TMDB API key ([get one here](https://www.themoviedb.org/settings/api))

## Quick Start

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

**6. View dbt docs in your browser**

Open http://localhost:8080 to see the dbt documentation.

## Common Commands

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
- `data/seeds/*.csv` - Reference data

## Development Workflow

The Docker setup mounts your local code directories, so you can:
1. Edit code on your host machine
2. Run changes immediately inside the container (no rebuild needed)

```bash
# In one terminal: keep the container running
docker compose up

# In another terminal: make changes, then test
docker compose exec tmdb-analytics python -m tmdb_ingestion.jobs.discover_movies --start-year 2024 --end-year 2024
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
Edit `docker compose.yml` and change the port mapping:
```yaml
ports:
  - "8081:8080"  # Use 8081 on your host instead
```

## Alternative: Using Dockerfile Only

If you prefer not to use Docker Compose:

```bash
# Build the image
docker build -t tmdb-analytics .

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