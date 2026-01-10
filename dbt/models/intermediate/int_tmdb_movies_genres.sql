--int_tmdb_movies_genres.sql

with base as (
    select
        movie_id,
        genres
    from {{ ref('stg_tmdb__movies') }}
),

unnested as (
    select
        b.movie_id,
        g.element.id as genre_id,
        g.element.name as genre_name
    from base as b
    cross join unnest(ifnull(b.genres.list, [])) as g
)

select
    movie_id,
    genre_id,
    genre_name
from unnested