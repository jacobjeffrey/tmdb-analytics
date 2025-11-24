-- bridge_movies_people.sql
-- Bridge table for movies and people (generated with stg_tmdb__cast). 
-- Contains character and cast order to simplify mart layer.
--
-- Grain: One entry for each unique casting
with base as (
    select person_id, character, cast_order, movie_id
    from {{ ref('stg_tmdb__credits') }}
)

select * from base