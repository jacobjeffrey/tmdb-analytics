-- models/intermediate/movies_genres.sql
with base as (
    select * from {{ ref('stg_movies') }}
)

select 
    m.movie_id,
    (jsonb_array_elements(m.genre_ids))::int as genre_ids
from base m

