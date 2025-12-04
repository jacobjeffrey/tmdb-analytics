-- bridge_movies_genres.sql

with source as (
    select
        movie_id,
        genre_id,
    from {{ ref('int_tmdb_movies_genres') }}
)

select * from source