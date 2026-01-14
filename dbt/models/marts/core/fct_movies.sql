-- fct_movies.sql

with base as (
    select
        movie_id,
        revenue,
        budget,
        runtime,
        popularity,
        vote_average,
        vote_count
    from {{ ref('stg_tmdb__movies' )}}
)

select * from base