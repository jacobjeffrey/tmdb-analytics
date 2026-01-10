-- models/staging/stg_tmdb__movies.sql

with src as (
    select
        movie_id,
        payload_json,
        ingested_at
    from {{ source('tmdb', 'movies') }}
),

-- Handle pagination quirks
deduped as (
    select
        movie_id,
        payload_json as p,
        ingested_at
    from src
    qualify row_number() over (
        partition by movie_id
        order by ingested_at desc
    ) = 1
)

select
    -- Primary key
    movie_id,

    -- Foreign keys
    p.imdb_id,

    -- Core identifiers
    p.title,
    p.original_title,

    -- Important dimensions
    p.status,
    safe_cast(p.release_date as date) as release_date,
    lower(p.original_language) as original_language,
    safe_cast(p.adult as bool) as adult,

    -- Key metrics
    safe_cast(p.revenue as int64) as revenue,
    safe_cast(p.budget as int64) as budget,
    safe_cast(p.runtime as int64) as runtime,
    safe_cast(p.popularity as float64) as popularity,
    safe_cast(p.vote_average as float64) as vote_average,
    safe_cast(p.vote_count as int64) as vote_count,

    -- Descriptive content
    p.overview,
    p.tagline,
    p.homepage,

    -- Complex/JSON (keep as-is; still RECORD/ARRAY types)
    p.belongs_to_collection,
    p.genres,
    p.origin_country,
    p.production_companies,
    p.production_countries,
    p.spoken_languages,

    -- Boolean flags
    safe_cast(p.video as bool) as video,

    -- Media paths
    p.backdrop_path,
    p.poster_path,

    -- Auditing
    ingested_at
from deduped
