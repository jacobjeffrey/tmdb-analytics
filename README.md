# TMDB Movie Analytics

> An end-to-end analytics engineering platform featuring TMDB API data.

# What This Is

This is an end-to-end analytics pipeline built around movie data from the 2000s onward. I chose this domain because I enjoy movie analytics and wanted something meaningful to practice with. The project pulls data from the TMDB API, loads it into Postgres, and transforms it with dbt into a clean star schema that supports analyses on genres, actors, and box office performance.

With this pipeline, you can run analyses such as:
- Highest-ROI genres since the 2000s (revenue/budget)
- Top-grossing actors or studios
- Currently most popular actors/movies

These queries become straightforward because the project uses:
- dbt to clean, test, and structure the raw API output
- A Kimball-style star schema to flatten TMDB’s heavily nested records into well-defined dimensions, fact tables, and bridge tables
- Bridge tables for many-to-many fields like credits and genres, eliminating the need to unnest JSON arrays at query time
- Data quality tests that catch common TMDB API issues such as missing people records or duplicated fields
## Current Status

**What's working:** Core pipeline complete - Asynchronous API ingestion, dbt transformations with 17+ models, dimensional modeling with bridge tables, comprehensive data quality tests.

**What I'm working on:** Refactoring ingestion to be modular

**What's next:** Either adding orchestration or migrating to GCP

## Tech Stack

- **Data Source:** [TMDB API](https://developer.themoviedb.org/docs/getting-started)
- **Storage:** DuckDB
- **Transformation:** dbt core
- **Frontend:** Rill (tentative) 


## How It Works

This is a straightforward ELT pipeline: extract from TMDB API, load raw data into Parquet, transform with dbt, and visualize in Rill.

```
TMDB API → Python Extraction → CSV (intermediate) → Rill (raw) → dbt Transformations → Rill
```

**1. Data Ingestion (Python → Parquet)**

Fetch data from TMDB API endpoints (movies, credits, people, genres, etc.) and save to local CSV files first. I cached to CSV so I can explore the output in Jupyter to do quick EDA and validation without having to repeatedly make calls.

The extraction uses async requests to handle tens of thousands of records efficiently while respecting TMDB's rate limit (~40 requests per second). Added retry logic with exponential backoff for timeouts and network hiccups. Implemented with asyncio/ahttp and tenacity.

Once validated, CSV files are bulk-loaded into Postgres raw tables.


**2. Data Transformation (dbt)**

**Staging layer** - Clean and standardize raw data (rename fields, fix data types, handle nulls). One staging model per raw source table.

**Intermediate layer** - Unnest some JSON fields to create new tables.

**Marts layer** - Build analytics-ready models:
- **Dimensions**: movies, people, genres, countries, languages, production companies
- **Facts**: movie performance metrics (revenue, budget, ratings)
- **Bridge tables**: many-to-many relationships for cast and genres

I used bridge tables for cast and genres (queried constantly) and kept production countries as JSON (rarely filtered).

Every model has tests: primary key uniqueness, not-null constraints, referential integrity checks.

**3. Analytics Frontend (Rill) - WIP**

Building interactive dashboards to explore trends like ROI by genre, actor career earnings, and budget evolution over time.
## Data Models

The project uses a **Kimball-inspired star schema** optimized for movie analytics queries.

**Why star schema:**
Analytical queries like "revenue by genre" or "top actors by box office" are just a couple of joins in a star schema vs 6-7 joins if everything was normalized. TMDB's data maps cleanly to dimensions and facts, and BI tools expect this structure.

**Design decisions:**
- **Bridge tables for cast, genres, and production_companies** - These many-to-many relationships are queried constantly ("top grossing actors", "best performing genres"), so I normalized them into proper bridge tables
- **Exploded production companies from movie details** - TMDB's production_company endpoint was redundant with data already in movie_details, so I extracted and deduped companies directly from the nested JSON arrays in the intermediate layer
- **JSON for less common fields** - Kept some fields such as 'spoken_languages' or 'production_countries' as JSON in `dim_movies` rather than creating additional bridge tables. These may not be queried as often and may clutter the mart layer.
- **Popularity in `dim_people`** - Technically a metric, but currently used as a snapshot in time, so a dedicated table is unneeded for now. May change in the future if I decide to track and implement SCD2.

**Key models:**

**Dimensions:**
- `dim_movies` - Core movie info: title, release date, runtime, budget, revenue, ratings
- `dim_people` - Cast and crew: name, biography, popularity
- `dim_genres`, `dim_languages`, `dim_countries`, `dim_production_companies` - Reference data

**Facts:**
- `fact_movies` - Movie performance metrics: revenue, budget, vote counts (one row per movie)

**Bridges:**
- `bridge_movies_cast` - Movies ↔ People with role details (character, cast order)
- `bridge_movies_genres` - Movies ↔ Genres (many-to-many)
- `bridge_movies_production_companies` - Movies ↔ Production Companies (many-to-many)

![dbt lineage graph](docs/images/dbt-dag.png)

## Getting Started

### You'll Need

- Python 3.8+
- Docker & Docker Compose
- dbt Core (`pip install dbt-postgres`)
- A TMDB API key ([grab one here](https://www.themoviedb.org/settings/api))

### Setup

**1. Clone and install**
```bash
git clone https://github.com/jacobjeffrey/tmdb-analytics.git
cd tmdb-analytics
pip install -r requirements.txt
```

**2. Environment config**
```bash
cp .env.example .env
# Edit .env with your TMDB API key and database credentials
```

**3. Start the database**
```bash
docker-compose up -d

# Check it's running
docker ps
```

**4. Configure dbt**

Copy the example profiles file and fill in your credentials:
```bash
cp profiles.yml.example ~/.dbt/profiles.yml
# Edit with your actual database credentials
```

Or manually create `~/.dbt/profiles.yml`:
```yaml
movie_data_analysis:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: your_username
      password: your_password
      dbname: your_database_name
      schema: your_schema_name
      threads: 4
```

**5. Run the pipeline**
```bash
# Pull data from TMDB
python ingestion/ingest_tmdb.py

# Transform with dbt
cd dbt
dbt run
dbt test

# Check out the docs
dbt docs generate
dbt docs serve  # Opens on localhost:8080

## Project Structure

```
├── dbt/
│   ├── models/
│   │   ├── staging/      # Staging models and tests
│   │   ├── intermediate/ # Intermediate models and tests
│   │   └── marts/        # Mart models (dim/fact/bridge) and tests
│   └── dbt_project.yml
├── ingestion/            # API Fetching and Database Loading
├── frontend/             # TODO
├── notebooks/            # Jupyter notebooks for quick EDA and data validation
```

## Things I Learned / Gotchas

- [Something that was harder than expected]
- [A library or pattern that saved you time]
- [A design decision you'd change if starting over]
- [An annoying bug or API quirk you had to work around]

Example:
- "TMDB's rate limiting is aggressive - had to add exponential backoff"
- "Bridge tables felt like overkill at first but make genre queries so much cleaner"
- "Wish I'd used Parquet from the start instead of JSON dumps"

## Running This Yourself

[Any specific notes for someone who wants to clone and run this?]

[Are you open to contributions? Is this just a learning project?]

## Credits

This product uses the TMDB API but is not endorsed or certified by TMDB.

![The Movie Database (TMDB)](docs/images/tmdb-logo.svg).
---