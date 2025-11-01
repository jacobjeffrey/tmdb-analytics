-- bridge_movies_cast.sql
with base as (
    select person_id, character, cast_order, movie_id
    from {{ ref('stg_tmdb__cast') }}
)

select * from base