# TMDB Movie Analytics Platform

> End-to-end analytics platform for movie industry data powered by The Movie Database (TMDB) API

## 🎯 Project Overview

This project builds a complete analytics pipeline from TMDB API ingestion through dimensional modeling to visualization, enabling comprehensive movie industry analysis including box office performance, cast analytics, genre trends, and production company insights.

## 📊 Current Status

### ✅ Completed
- **Data Ingestion**: TMDB API extraction pipeline built and tested
- **Data Transformation**: 20+ dbt models implemented with full test coverage
- **Dimensional Modeling**: Star schema with facts, dimensions, and bridge tables
- **Data Quality**: Comprehensive dbt tests ensuring data integrity

### 🚧 In Progress
- **Analytics Frontend**: Building interactive dashboards and visualizations

### 📋 Planned
- Refactor ingestion to be modular and use parquet
- Automated daily refresh and reporting

## 🏗️ Architecture

```
TMDB API → Ingestion Layer → Raw Data → Staging → Intermediate → Marts → Frontend (WIP)
```

**Tech Stack:**
- **Ingestion**: [Python/your tool]
- **Storage**: [Snowflake/BigQuery/Postgres/Redshift]
- **Transformation**: dbt Core
- **Frontend**: [React/Streamlit/Tableau/Looker] *(planned)*
- **Orchestration**: [Airflow/Prefect/dbt Cloud] *(if applicable)*

## 📊 Data Models

### Data Modeling Approach

The project uses a **Kimball-inspired dimensional model** optimized for movie analytics queries.

**Design decisions:**
- Mostly Star schema with fact and dimension tables
- Bridge tables for high-cardinality, frequently-queried relationships (genres, cast)
- Semi-structured data retained for lower-priority attributes (languages, production countries)
- Optimized for common analytical patterns: genre performance, cast analysis, box office trends

### Model Architecture

The project follows a layered approach with clear separation of concerns:

**Raw Layer** (`raw_*`)
- Unprocessed JSON responses from TMDB API
- Preserves source data exactly as received
- Models: `raw.movies`, `raw.people`, `raw.credits`, `raw.countries`, `raw.genres`, `raw.languages`, `raw.movie_details`

**Staging Layer** (`stg_tmdb_*`)
- Cleaned and typed source data
- Standardized column names and data types
- Basic deduplication and filtering
- Models: `stg_tmdb_movie_details`, `stg_tmdb_people`, `stg_tmdb_credits`, `stg_tmdb_countries`, `stg_tmdb_genres`, `stg_tmdb_languages`

**Intermediate Layer** (`int_*`)
- Unnesting of semi-structured data (e.g., exploding production companies from JSON arrays)
- Models: `int_production_companies`

**Marts Layer** (Analytics-Ready)
- **Dimensions**: 
  - `dim_movies` - Core movie attributes (title, release date, runtime, budget, revenue)
  - `dim_people` - Cast information
  - `dim_countries` - Country reference data
  - `dim_genres` - Genre classifications
  - `dim_languages` - Language reference data
  - `dim_production_companies` - Production company details
  
- **Facts**: 
  - `fact_movies` - Movie performance metrics and aggregated stats
  
- **Bridge Tables** (Many-to-Many relationships):
  - `bridge_movies_cast` - Movies ↔ Actors/Crew relationships
  - `bridge_movies_genres` - Movies ↔ Genres relationships

### dbt Lineage

![dbt Lineage Graph](docs/images/dbt-dag.png)

The lineage graph above shows the complete data flow from raw TMDB API data through to analytics-ready dimensional models.

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- [Your database]
- dbt Core installed (`pip install dbt-[adapter]`)
- TMDB API key ([Get one here](https://www.themoviedb.org/settings/api))

### Installation

1. Clone the repository
```bash
git clone [your-repo-url]
cd tmdb-analytics
```

2. Install dependencies
```bash
pip install -r requirements.txt
```

3. Set up environment variables
```bash
cp .env.example .env
# Edit .env with your TMDB API key and database credentials
```

4. Configure dbt profiles
```bash
# Edit ~/.dbt/profiles.yml with your database connection
```

### Running the Pipeline

**Ingest data from TMDB API:**
```bash
python scripts/ingest_tmdb.py
```

**Run dbt transformations:**
```bash
# Run all models
dbt run

# Run specific layer
dbt run --select staging
dbt run --select marts

# Run with full refresh
dbt run --full-refresh
```

**Run data quality tests:**
```bash
dbt test
```

**Generate documentation:**
```bash
dbt docs generate
dbt docs serve
```

## 📁 Project Structure

```
├── dbt/
│   ├── models/
│   │   ├── staging/        # stg_tmdb_* models
│   │   ├── intermediate/   # int_* models
│   │   └── marts/          # dim_*, fact_*, bridge_* models
│   ├── tests/              # Custom dbt tests
│   ├── macros/             # Reusable SQL macros
│   └── dbt_project.yml
├── ingestion/
│   ├── tmdb_client.py      # TMDB API wrapper
│   ├── extract.py          # Data extraction logic
│   └── config/
│       └── endpoints.yml   # API endpoint configurations
├── frontend/               # (Coming soon)
├── docs/
│   └── images/
│       └── dbt_lineage.png
├── scripts/
│   └── ingest_tmdb.py      # Main ingestion script
├── tests/                  # Python unit tests
├── .env.example
├── requirements.txt
└── README.md
```

## 🧪 Testing & Data Quality

dbt tests validate:
- **Uniqueness**: Primary keys in all dimension tables
- **Referential integrity**: Foreign key relationships between facts and dimensions
- **Not null constraints**: Required fields across all models
- **Accepted values**: Genre codes, language codes, country codes
- **Custom business logic**: Revenue vs budget validation, date ranges, etc.

Run tests:
```bash
# All tests
dbt test

# Specific model
dbt test --select dim_movies

# Specific test type
dbt test --select test_type:unique
```

## 📈 Example Analytics Use Cases

With this data warehouse, you can answer questions like:

- What are the highest-grossing movies by genre over time?
- Which production companies have the best ROI?
- How do movie runtimes correlate with box office performance?
- What are the career earnings for specific actors?
- Which countries produce the most movies by genre?
- How have movie budgets evolved across decades?

## 📖 Documentation

View the full dbt documentation with model descriptions, column definitions, and lineage:

```bash
dbt docs generate
dbt docs serve
```

Then navigate to `http://localhost:8080`

## 🤝 Contributing

This is a personal learning project, but suggestions and feedback are welcome! Feel free to open an issue.

## 📝 License

[Your chosen license - MIT/Apache/All Rights Reserved]

## 🗺️ Roadmap

**Phase 1: Core Pipeline** ✅
- [x] TMDB API ingestion
- [x] dbt dimensional models
- [x] Data quality tests

**Phase 2: Frontend** 🚧
- [ ] Interactive dashboard framework
- [ ] Key metric visualizations
- [ ] Filtering and drill-down capabilities

**Phase 3: Advanced Features** 📋
- [ ] Real-time data refresh
- [ ] Predictive analytics (box office forecasting)
- [ ] Sentiment analysis from reviews
- [ ] User recommendation engine
- [ ] Export and sharing functionality

**Phase 4: Production** 📋
- [ ] Automated orchestration
- [ ] Cloud deployment
- [ ] Monitoring and alerting
- [ ] Performance optimization

---

**Note**: This project is under active development. The data pipeline (ingestion + dbt) is stable and tested, but the analytics frontend is still being built.

**Data Source**: This project uses the TMDB API but is not endorsed or certified by TMDB.