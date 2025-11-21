# TMDB Movie Analytics Platform

> An end-to-end analytics engineering platform featuring TMDB API data.

## What This Is

I built this project to familiarize myself with dbt and get some SQL practice in. And since I've been into movies lately, I decided to combine my interests and use TMDB's api data to build this analytics pipeline. It will eventually feature dashboards on trends such as ROI per genre, most popular actors today, etc.

## Current Status

**What's working:** Just finished the dbt models and tests, which revealed issues in my ingestion logic. In particular, I learned some records in the 'credit' endpoint don't have a corresponding 'people' entry, requiring me to fix how retries and invalid responses are handled.

**What I'm working on:** After finishing this readme, I'll start working on an analytics dashboard with Streamlit.

**What's next:** Once the dashboard is done, I'll refactor the ingestion layer. It works, but it's super repetitive and could reworked into a modular ingestion class. I also currently use CSV, which I learned can be quite a pain to use when your data has odd characters or quotes. I will most likely use Parquet instead. Lastly, I'll add some light orchestration. According to [TMDB staff](https://www.themoviedb.org/talk/65eba9f36cf3d501844557ea), the API should have new data by 08:00 UTC daily, so it should be simple to implement.

## Tech Stack

- **Data Source:** [TMDB API](https://developer.themoviedb.org/docs/getting-started)
- **Storage:** Postgres (running in Docker)
- **Transformation:** dbt/other
- **Frontend:** Streamlit 


## How It Works

The project is essentially a fairly forward data pipeline, starting with TMDB API ingestion into local CSV files. The data from these files is then read into the Postgres instance running on Docker. From there, the data is transformed into staging, intermediate, and mart layers with dbt. The models from these layers then feed into the upcoming Streamlit dashboard.

```
[TMDB API] → [Ingestion] → [Raw Files (CSV)] → [Database/Raw Schema] → [dbt Models] → [Streamlit]
```

[Explain each step briefly - what happens and why?]

## Data Models

[Explain your modeling approach - why star schema? Why bridge tables? What tradeoffs did you make?]

Example: "I went with a Kimball-style star schema because [reason]. Used bridge tables for [relationships] since [reason]. Kept [some data] as JSON because [reason]."

**Key models:**
- `dim_[entity]` - [What this dimension represents]
- `fact_[entity]` - [What metrics this captures]
- `bridge_[relationship]` - [What many-to-many relationship this handles]

[Optional: Include your dbt lineage graph if you have one]

## Getting Started

### You'll Need

- Python 3.8+
- Docker & Docker Compose
- dbt Core (`pip install dbt-postgres`)
- A TMDB API key ([grab one here](https://www.themoviedb.org/settings/api))

### Setup

**1. Clone and install**
```bash
git clone [your-repo-url]
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
Edit `~/.dbt/profiles.yml` or use the one in the project:
```yaml
tmdb_analytics:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: tmdb_user
      password: tmdb_password
      dbname: tmdb_analytics
      schema: public
      threads: 4
```

**5. Run the pipeline**
```bash
# Pull data from TMDB
python scripts/ingest_tmdb.py

# Transform with dbt
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
│   │   ├── staging/      # [What these do]
│   │   ├── intermediate/ # [What these do]
│   │   └── marts/        # [What these do]
│   └── dbt_project.yml
├── ingestion/            # [What's in here]
├── frontend/             # [Current status]
├── scripts/              # [Utility scripts]
└── tests/                # [What you're testing]
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