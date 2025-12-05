-- int_tmdb_cast_crew_combined

with cast_credits as (
    SELECT
        credit_id,
        movie_id,
        person_id,
        name,
        original_name,
        gender,
        known_for_department,
        popularity,
        credit_type,
        character,
        cast_order,
        null as job,
        null as department
    from {{ ref('stg_tmdb__cast')}}
),

crew_credits as (
    SELECT
        credit_id,
        movie_id,
        person_id,
        name,
        original_name,
        gender,
        known_for_department,
        popularity,
        credit_type,
        null as character,
        null as cast_order,
        job,
        department
    from {{ ref('stg_tmdb__crew')}}
)

select
    credit_id,
    movie_id,
    person_id,
    name,
    original_name,
    gender,
    known_for_department,
    popularity,
    credit_type,
    character,
    cast_order,
    job,
    department
from cast_credits

union all

select
    credit_id,
    movie_id,
    person_id,
    name,
    original_name,
    gender,
    known_for_department,
    popularity,
    credit_type,
    character,
    cast_order,
    job,
    department
from crew_credits