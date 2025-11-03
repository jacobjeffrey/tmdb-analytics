-- bridge_movies_cast.sql
-- Bridge table for movies and cast members (generated with stg_tmdb__cast)
--
-- Grain: One entry for each unique casting
with base as (
    select person_id, character, cast_order, movie_id
    from {{ ref('stg_tmdb__cast') }}
)

select * from base