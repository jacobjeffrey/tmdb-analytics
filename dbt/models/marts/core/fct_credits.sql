-- fct_credits.sql

with base as (
    select
        credit_id,
        movie_id,
        person_id,
        credit_type,
        character,
        cast_order,
        job,
        department
    from {{ ref('int_tmdb_cast_crew_combined')}}
)

select * from base