-- bridge_movies_origin_countries.sql

with base as (
    select
        movie_id,
        origin_country_code
    from {{ ref('int_tmdb_movies_origin_countries') }}
)

select * from base