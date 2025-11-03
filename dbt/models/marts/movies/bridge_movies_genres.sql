-- bridge_movies_genres.sql
-- Bridge table for movies and genres (generated with stg_tmdb__movies)
--
-- Grain: One entry for each movie_id and genre_id combo
--
-- Transformations:
-- - Explode the genre_ids JSON field
with base as (
    select movie_id, genre_ids
    from {{ ref('stg_tmdb__movies') }}
)

select 
    m.movie_id,
    (jsonb_array_elements(m.genre_ids))::int as genre_ids
from base m

