# TMDB Movie Analytics

> An end-to-end analytics engineering platform featuring TMDB API data.

## What This Is

I built this to learn dbt and dimensional modeling using data I actually find interesting. The pipeline extracts movie data from TMDB's API, transforms it with dbt into a star schema, and will eventually power Streamlit dashboards analyzing genre performance, actor trends, and box office patterns.

## Current Status

**What's working:** Core pipeline complete - API ingestion, dbt transformations with 20+ models, dimensional modeling with bridge tables, comprehensive data quality tests.

**What I'm working on:** Building Streamlit dashboards to visualize the data.

**What's next:** Refactor ingestion to be modular and use Parquet instead of CSV (learned the hard way about quote/encoding issues), then add daily orchestration since TMDB updates at 08:00 UTC.

## Tech Stack

- **Data Source:** [TMDB API](https://developer.themoviedb.org/docs/getting-started)
- **Storage:** Postgres (running in Docker)
- **Transformation:** dbt/other
- **Frontend:** Streamlit 


## How It Works

This is a straightforward ELT pipeline: extract from TMDB API, load raw data into Postgres, transform with dbt, and visualize in Streamlit.

```
TMDB API → Python Extraction → CSV (intermediate) → PostgreSQL (raw) → dbt Transformations → Streamlit
```

**1. Data Ingestion (Python → CSV → PostgreSQL)**

Fetch data from TMDB API endpoints (movies, credits, people, genres, etc.) and save to local CSV files first. I cache to CSV so I can explore the data in Jupyter and explore the output and do quick validation checks.

The extraction uses async requests to handle tens of thousands of records efficiently while respecting TMDB's rate limit (~40 requests per second). Added retry logic with exponential backoff for timeouts and network hiccups.

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

**3. Analytics Frontend (Streamlit) - WIP**

Building interactive dashboards to explore trends like ROI by genre, actor career earnings, and budget evolution over time.
## Data Models

The project uses a **Kimball-inspired star schema** optimized for movie analytics queries.

**Why star schema:**
Analytical queries like "revenue by genre" or "top actors by box office" are just a couple of joins in a star schema vs 6-7 joins if everything was normalized. TMDB's data maps cleanly to dimensions and facts, and BI tools expect this structure.

**Design decisions:**
- **Bridge tables for cast and genres** - These many-to-many relationships are queried constantly ("top grossing actors", "best performing genres"), so I normalized them into proper bridge tables
- **Exploded production companies from movie details** - TMDB's production_company endpoint was redundant with data already in movie_details, so I extracted and deduped companies directly from the nested JSON arrays in the intermediate layer
- **JSON for less common fields** - Kept some fields such as 'spoken_languages' or 'production_countries' as JSON in `dim_movies` rather than creating additional bridge tables.
- **Popularity in `dim_people`** - Technically a metric, but it's a snapshot value used for filtering/context rather than time-series analysis. If I track popularity over time, I'd refactor to SCD Type 2 or a separate fact table

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
```

**Docker tips:**
```bash
# Stop everything
docker-compose down

# View logs
docker-compose logs -f postgres

# Access PostgreSQL directly
docker exec -it tmdb-postgres psql -U tmdb_user -d tmdb_analytics

# Nuclear option (deletes all data)
docker-compose down -v
```

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

[This section makes it super human - share your struggles and discoveries!]

- [Something that was harder than expected]
- [A library or pattern that saved you time]
- [A design decision you'd change if starting over]
- [An annoying bug or API quirk you had to work around]

Example:
- "TMDB's rate limiting is aggressive - had to add exponential backoff"
- "Bridge tables felt like overkill at first but make genre queries so much cleaner"
- "Wish I'd used Parquet from the start instead of JSON dumps"

## What You Can Do With This

[List 3-5 example queries or analyses this enables]

Example:
- Find the highest-grossing [metric] by [dimension]
- Compare [thing] across [time period]
- Analyze [relationship] between [X] and [Y]

## Roadmap

**Now:** [What you're actively working on]

**Next:** [What's coming after current work]

**Maybe Eventually:** [Nice-to-haves or moonshots]

[Keep this casual - it's okay to say "might add X if I feel like it" or "would be cool to try Y"]

## Running This Yourself

[Any specific notes for someone who wants to clone and run this?]

[Are you open to contributions? Is this just a learning project?]

## Credits / Notes

- Data from [TMDB API](https://www.themoviedb.org/) - [Any API terms you need to mention]
- [Any tutorials, blog posts, or projects that inspired you]
- [Shoutout to anyone who helped or gave feedback]

---

[Optional: Add a personal note about what you learned or what you're proud of]