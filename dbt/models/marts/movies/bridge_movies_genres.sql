-- bridge_movies_genres.sql
with movies_genres as (
    select * from {{ ref('int_movies_genres') }}
)

select * from movies_genres