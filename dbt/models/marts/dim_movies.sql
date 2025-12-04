-- dim_movies.sql

with base as (
    select
        movie_id,
        imdb_id,
        title,
        original_title,
        status,
        release_date,
        original_language,
        overview,
        tagline,
        homepage,
        belongs_to_collection,
        backdrop_path,
        poster_path
    from {{ ref('stg_tmdb__movies') }}
)

select * from base