SHELL := /bin/bash
DC ?= docker compose

.PHONY: help build up down restart logs shell clean ingest ingest-2024 ingest-seeds \
        dbt-deps dbt-seed dbt-run dbt-test dbt-docs dbt-clean pipeline init rebuild check

# Default target
help:
	@echo "TMDB Analytics - Available Commands"
	@echo "===================================="
	@echo ""
	@echo "Setup & Control:"
	@echo "  make build          Build Docker image"
	@echo "  make up             Start containers in background"
	@echo "  make down           Stop and remove containers"
	@echo "  make restart        Restart containers"
	@echo "  make logs           View container logs"
	@echo "  make shell          Enter container shell"
	@echo "  make clean          Remove containers, volumes, and data"
	@echo ""
	@echo "Data Pipeline:"
	@echo "  make ingest         Run full TMDB data ingestion (~10 min)"
	@echo "  make ingest-2024    Ingest only 2024 data (faster)"
	@echo "  make ingest-seeds   Update seed data (genres, countries, languages)"
	@echo "  make dbt-deps       Install dbt packages"
	@echo "  make dbt-seed       Load reference data (genres, countries, etc.)"
	@echo "  make dbt-run        Run dbt transformations"
	@echo "  make dbt-test       Run dbt data quality tests"
	@echo "  make dbt-docs       Generate and serve dbt docs (port 8080)"
	@echo "  make pipeline       Run complete pipeline (ingest + dbt)"
	@echo ""
	@echo "Development:"
	@echo "  make init           First-time setup (build + up)"
	@echo "  make rebuild        Clean rebuild from scratch"
	@echo ""
	@echo "Config:"
	@echo "  DC=<command>        Override compose command (default: 'docker compose')"
	@echo "                     e.g., make DC=docker-compose up"

# Setup & Control
build:
	@echo "Building Docker image..."
	$(DC) build

up:
	@echo "Starting containers..."
	$(DC) up -d
	@echo "Containers started. Run 'make shell' to enter the container."

down:
	@echo "Stopping containers..."
	$(DC) down

restart: down up

logs:
	$(DC) logs -f

shell:
	$(DC) exec tmdb-analytics bash

clean:
	@echo "WARNING: This will remove all containers, volumes, and local data outputs!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		$(DC) down -v; \
		rm -rf data/*.parquet data/*.db data/movies/* data/movie_details/*; \
		echo "Cleanup complete."; \
	fi

# Data Ingestion
ingest:
	@echo "Running full TMDB data ingestion (this takes ~10 minutes)..."
	$(DC) exec tmdb-analytics python -m tmdb_ingestion.ingest_tmdb

ingest-2024:
	@echo "Ingesting 2024 movies only..."
	$(DC) exec tmdb-analytics python -m tmdb_ingestion.jobs.discover_movies --start-year 2024 --end-year 2024
	$(DC) exec tmdb-analytics python -m tmdb_ingestion.jobs.fetch_movie_details

ingest-seeds:
	@echo "Updating seed data (genres, countries, languages)..."
	$(DC) exec tmdb-analytics python -m tmdb_ingestion.jobs.update_seeds

# dbt Commands
dbt-deps:
	@echo "Installing dbt packages..."
	$(DC) exec tmdb-analytics bash -c "cd dbt && dbt deps"

dbt-seed:
	@echo "Loading reference data..."
	$(DC) exec tmdb-analytics bash -c "cd dbt && dbt seed"

dbt-run:
	@echo "Running dbt transformations..."
	$(DC) exec tmdb-analytics bash -c "cd dbt && dbt run"

dbt-test:
	@echo "Running dbt tests..."
	$(DC) exec tmdb-analytics bash -c "cd dbt && dbt test"

dbt-docs:
	@echo "Generating and serving dbt docs..."
	@echo "Documentation will be available at http://localhost:8080"
	$(DC) exec tmdb-analytics bash -c "cd dbt && dbt docs generate && dbt docs serve --host 0.0.0.0"

dbt-clean:
	@echo "Cleaning dbt artifacts..."
	$(DC) exec tmdb-analytics bash -c "cd dbt && dbt clean"

# Complete Pipeline
pipeline:
	@echo "Running complete pipeline..."
	@$(MAKE) ingest
	@$(MAKE) dbt-deps
	@$(MAKE) dbt-seed
	@$(MAKE) dbt-run
	@$(MAKE) dbt-test
	@echo "Pipeline complete! Run 'make dbt-docs' to view documentation."

# Development Helpers
init: build up
	@echo "First-time setup complete!"
	@echo "Next steps:"
	@echo "  1. Ensure your .env file has your TMDB API key"
	@echo "  2. Run 'make pipeline' to execute the full data pipeline"
	@echo "  3. Run 'make dbt-docs' to view the documentation"

rebuild:
	@echo "Rebuilding from scratch..."
	$(DC) down -v
	$(DC) build --no-cache
	$(DC) up -d
	@echo "Rebuild complete."

# Check if containers are running
check:
	@$(DC) ps
