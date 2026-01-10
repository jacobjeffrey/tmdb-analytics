SHELL := /bin/bash
DC ?= docker compose

.PHONY: help build up down restart logs shell clean ingest ingest-2026 ingest-seeds \
        dbt-deps dbt-seed dbt-run dbt-test dbt-docs dbt-clean pipeline init rebuild check

# Default target
help:
	@echo "TMDB Analytics - Available Commands"
	@echo "===================================="
	@echo ""
	@echo "Setup & Control:"
	@echo "  make build              Build Docker image"
	@echo "  make up                 Start containers in background"
	@echo "  make down               Stop and remove containers"
	@echo "  make restart            Restart containers"
	@echo "  make logs               View container logs"
	@echo "  make shell              Enter container shell"
	@echo "  make clean              Remove containers, volumes, and data"
	@echo ""
	@echo "Data Pipeline:"
	@echo "  make ingest             Run full TMDB data ingestion (~10 min)"
	@echo "  make ingest-2026        Ingest only 2026 data (faster)"
	@echo "  make ingest-seeds       Update seed data (genres, countries, languages)"
	@echo ""
	@echo "dbt (Transformations):"
	@echo "  make dbt-deps           Install dbt packages"
	@echo "  make dbt-seed           Load reference seed data"
	@echo "  make dbt-run            Run all dbt models"
	@echo "  make dbt-run-staging    Run staging models only"
	@echo "  make dbt-run-intermediate Run intermediate models only"
	@echo "  make dbt-run-marts      Run marts models only"
	@echo "  make dbt-test           Run dbt tests"
	@echo "  make dbt-docs           Generate and serve dbt docs (port 8080)"
	@echo "  make dbt-clean          Clean dbt artifacts"
	@echo ""
	@echo "Model Selection (advanced):"
	@echo "  make dbt-run SEL=dim_movies"
	@echo "  make dbt-run SEL=+path:models/marts     (marts + dependencies)"
	@echo "  make dbt-run SEL=path:models/staging    (staging only)"
	@echo "  make dbt-run SEL=tag:daily EXC=tag:slow"
	@echo ""
	@echo "Pipelines:"
	@echo "  make pipeline           Run ingest + dbt (all models)"
	@echo "  make pipeline SEL=+path:models/marts    (marts only)"
	@echo ""
	@echo "Development:"
	@echo "  make init               First-time setup (build + up)"
	@echo "  make rebuild            Clean rebuild from scratch"
	@echo "  make check              Check running containers"
	@echo ""
	@echo "Config:"
	@echo "  DC=<command>            Override compose command (default: 'docker compose')"
	@echo "                          e.g., make DC=docker-compose up"

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

ingest-2026:
	@echo "Ingesting 2026 movies only..."
	$(DC) exec tmdb-analytics python -m tmdb_ingestion.jobs.discover_movies --start-year 2026 --end-year 2026
	$(DC) exec tmdb-analytics python -m tmdb_ingestion.jobs.fetch_movie_details

ingest-seeds:
	@echo "Updating seed data (genres, countries, languages)..."
	$(DC) exec tmdb-analytics python -m tmdb_ingestion.jobs.update_seeds

# dbt Commands
SEL ?=
EXC ?=
DBT_TARGET ?=
DBT_VARS ?=

dbt-run:
	@echo "Running dbt transformations..."
	$(DC) exec tmdb-analytics bash -c 'cd dbt && dbt run \
		$(if $(DBT_TARGET),--target $(DBT_TARGET),) \
		$(if $(SEL),--select "$(SEL)",) \
		$(if $(EXC),--exclude "$(EXC)",) \
		$(if $(DBT_VARS),--vars "$(DBT_VARS)",)'

dbt-run-staging:
	@echo "Running dbt staging models..."
	$(DC) exec tmdb-analytics bash -c 'cd dbt && dbt run --select "path:models/staging"'

dbt-run-intermediate:
	@echo "Running dbt intermediate models..."
	$(DC) exec tmdb-analytics bash -c 'cd dbt && dbt run --select "path:models/intermediate"'

dbt-run-marts:
	@echo "Running dbt marts models..."
	$(DC) exec tmdb-analytics bash -c 'cd dbt && dbt run --select "path:models/marts"'
	
dbt-deps:
	@echo "Installing dbt packages..."
	$(DC) exec tmdb-analytics bash -c "cd dbt && dbt deps"

dbt-seed:
	@echo "Loading reference data..."
	$(DC) exec tmdb-analytics bash -c "cd dbt && dbt seed"

dbt-test:
	@echo "Running dbt tests..."
	@echo "SEL='$(SEL)' EXC='$(EXC)' DBT_TARGET='$(DBT_TARGET)'"
	$(DC) exec tmdb-analytics bash -lc 'cd dbt && dbt test \
		$(if $(DBT_TARGET),--target $(DBT_TARGET),) \
		$(if $(SEL),--select "$(SEL)",) \
		$(if $(EXC),--exclude "$(EXC)",) \
		$(if $(DBT_VARS),--vars "$(DBT_VARS)",)'

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
