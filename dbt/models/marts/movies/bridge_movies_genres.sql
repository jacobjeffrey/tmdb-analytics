-- bridge_movies_genres.sql
-- Bridge table for movies and genres (generated with stg_tmdb__movie_details)
--
-- Grain: One entry for each movie_id and genre_id combo
--
-- Transformations:
-- - Explode the genre JSON field
with base as (
    select movie_id, genres
    from {{ ref('stg_tmdb__movie_details') }}
)

select
    movie_id,
    (g ->> 'id')::bigint as genre_id
from base
cross join lateral jsonb_array_elements(base.genres) as g