-- stg_tmdb__movies.sql

with src as (
    select
        movie_id,
        payload_json,
        ingested_at
    from {{ source('tmdb', 'movie_details_and_credits')}}
),

-- Handle pagination quirks
deduped as (
    select
        movie_id,
        payload_json as p,
        ingested_at
    from src
    QUALIFY row_number() over (
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
    p.release_date::date AS release_date,
    lower(p.original_language) AS original_language,
    p.adult::boolean AS adult,

    -- Key metrics
    p.revenue::bigint AS revenue,
    p.budget::integer AS budget,
    p.runtime::integer AS runtime,
    p.popularity::double AS popularity,
    p.vote_average::double AS vote_average,
    p.vote_count::bigint AS vote_count,

    -- Descriptive content
    p.overview,
    p.tagline,
    p.homepage,

    -- Complex/JSON
    p.belongs_to_collection,
    p.genres,
    p.origin_country,
    p.production_companies,
    p.production_countries,
    p.spoken_languages,

    -- Boolean flags
    p.video::boolean AS video,

    -- Media paths
    p.backdrop_path,
    p.poster_path,

    -- Auditing
    ingested_at
from deduped
