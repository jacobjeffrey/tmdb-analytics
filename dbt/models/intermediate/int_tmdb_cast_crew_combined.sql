-- int_tmdb_cast_crew_combined

with cast_credits as (
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
        cast(NULL as STRING) as job,
        cast(NULL as STRING) as department
    from {{ ref('stg_tmdb__cast')}}
),

crew_credits as (
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
        cast(NULL as STRING) as character,
        cast(NULL as INT64) as cast_order,
        job,
        department
    from {{ ref('stg_tmdb__crew')}}
)

select * from cast_credits
union all
select * from crew_credits