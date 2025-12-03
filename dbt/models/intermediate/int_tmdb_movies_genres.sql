--int_tmdb_movies_genres.sql

with base as (
    select 
        movie_id,
        genres
    from {{ ref('stg_tmdb__movies')  }}
),

unnested as (
    select
        movie_id,
        g.id as genre_id,
        g.name as genre_name
    from base,
    unnest(genres) as t(g)
)

select
    movie_id,
    genre_id,
    genre_name
from unnested