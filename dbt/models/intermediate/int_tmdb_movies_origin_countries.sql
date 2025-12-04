-- int_tmdb_movies_origin_country

with source as (
    select
        movie_id,
        origin_country
    from {{ ref('stg_tmdb__movies') }}
),

unnested as (
    select
        movie_id,
        unnest(origin_country) as origin_country_code
    from source
)

select
    movie_id,
    origin_country_code
from unnested