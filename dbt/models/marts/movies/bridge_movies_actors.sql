-- bridge_movies_actors.sql
with cast_movies as (
    select * from {{ ref('int_cast_movies') }}
)

select movie_id, person_id, character, cast_order from cast_movies