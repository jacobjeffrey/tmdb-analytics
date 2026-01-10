# dbt project overview

This folder contains the dbt project that transforms raw TMDB data into a star schema for analytics. For broader project setup, see the root [README](../README.md) and [DOCKER_SETUP](../DOCKER_SETUP.md).

## Model layers

The project follows a layered structure defined in `dbt_project.yml`:

- **staging** (`models/staging`) — cleans raw TMDB sources into consistent staging views (schema `stg`, materialized as views).
- **intermediate** (`models/intermediate`) — joins/unnests array-heavy fields into relational forms (schema `int`, materialized as views).
- **marts** (`models/marts`) — dimensional models and fact tables for analytics (schema `marts`, materialized as tables).

Seed data (genres, countries, languages) lives in `seeds/` and is loaded into the `seed_data` schema.

## Running dbt in this repo

All dbt commands are wrapped in the Makefile and executed inside the Docker container:

```bash
make dbt-deps
make dbt-seed
make dbt-run
make dbt-test
```

You can also target specific layers:

```bash
make dbt-run-staging
make dbt-run-intermediate
make dbt-run-marts
```

To run dbt manually inside the container:

```bash
docker compose exec tmdb-analytics bash
cd dbt
DBT_TARGET=dev_bq dbt run
```

For full pipeline instructions (including ingestion and docs), refer to the root [README](../README.md) and [DOCKER_SETUP](../DOCKER_SETUP.md).

## Profiles and targets

The dbt profile name is `tmdb_analytics` (see `dbt_project.yml`). Example targets are defined in the repo root `profiles.yml.example`:

- `dev_bq` (default target) — BigQuery using OAuth for development.
- `dev_duckdb` — DuckDB using `/app/data/tmdb_analytics.db` for local iteration.
- `prod` — BigQuery using a service account.

When using Makefile targets, set the target with `DBT_TARGET`:

```bash
DBT_TARGET=dev_duckdb make dbt-run
```

If you are running dbt locally (outside Docker), copy `profiles.yml.example` to `~/.dbt/profiles.yml` and adjust environment variables as needed.
