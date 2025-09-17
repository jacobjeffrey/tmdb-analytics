--- initialize the schemas ---
CREATE SCHEMA IF NOT EXISTS "raw";
CREATE SCHEMA IF NOT EXISTS "stg";
CREATE SCHEMA IF NOT EXISTS "int";
CREATE SCHEMA IF NOT EXISTS "marts";

--- initialize the tables ---
CREATE TABLE IF NOT EXISTS raw.movies (
    adult TEXT,
    backdrop_path TEXT,
    genre_ids TEXT,
    id TEXT,
    original_language TEXT,
    original_title TEXT,
    overview TEXT,
    popularity TEXT,
    poster_path TEXT,
    release_date TEXT,
    title TEXT,
    video TEXT,
    vote_average TEXT,
    vote_count TEXT
);

CREATE TABLE IF NOT EXISTS raw.movie_details (
    adult TEXT,
    backdrop_path TEXT,
    belongs_to_collection TEXT,
    budget TEXT,
    genres TEXT,
    homepage TEXT,
    id TEXT,
    imdb_id TEXT,
    origin_country TEXT,
    original_language TEXT,
    original_title TEXT,
    overview TEXT,
    popularity TEXT,
    poster_path TEXT,
    production_companies TEXT,
    production_countries TEXT,
    release_date TEXT,
    revenue TEXT,
    runtime TEXT,
    spoken_languages TEXT,
    status TEXT,
    tagline TEXT,
    title TEXT,
    video TEXT,
    vote_average TEXT,
    vote_count TEXT
);

CREATE TABLE IF NOT EXISTS raw.cast_members (
    adult TEXT,
    gender TEXT,
    id TEXT,
    known_for_department TEXT,
    name TEXT,
    original_name TEXT,
    popularity TEXT,
    profile_path TEXT,
    cast_id TEXT,
    character TEXT,
    credit_id TEXT,
    "order" TEXT,
    movie_id TEXT
);

CREATE TABLE IF NOT EXISTS raw.genres (
    id TEXT,
    name TEXT
);