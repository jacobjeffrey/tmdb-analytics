--- initialize the schemas ---
CREATE SCHEMA IF NOT EXISTS "raw";
CREATE SCHEMA IF NOT EXISTS "stg";
CREATE SCHEMA IF NOT EXISTS "int";
CREATE SCHEMA IF NOT EXISTS "marts";

--- initialize the tables ---
CREATE TABLE IF NOT EXISTS raw.movie_details (
    adult BOOLEAN,
    backdrop_path TEXT,
    belongs_to_collection JSONB,
    budget BIGINT,
    genres JSONB,
    homepage TEXT,
    id INTEGER PRIMARY KEY,
    imdb_id TEXT,
    origin_country JSONB,
    original_language TEXT,
    original_title TEXT,
    overview TEXT,
    popularity NUMERIC,
    poster_path TEXT,
    production_companies JSONB,
    production_countries JSONB,
    release_date DATE,
    revenue BIGINT,
    runtime INTEGER,
    spoken_languages JSONB,
    status TEXT,
    tagline TEXT,
    title TEXT,
    video BOOLEAN,
    vote_average NUMERIC,
    vote_count INTEGER
);


CREATE TABLE IF NOT EXISTS raw.credits (
    movie_id INTEGER,
    credits_json JSONB
);

