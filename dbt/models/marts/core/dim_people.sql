-- dim_people.sql

with base as (
    select
        person_id,
        name,
        original_name,
        gender,
        known_for_department,
        popularity as raw_popularity
    from {{ ref('int_tmdb_cast_crew_combined') }}
),

scored as (
    select
        person_id,
        name,
        original_name,
        gender,
        known_for_department,
        raw_popularity,
        round(
            avg(raw_popularity) over (partition by person_id),
            3
        ) as popularity
    from base
),

deduped as (
    select
        person_id,
        name,
        original_name,
        gender,
        known_for_department,
        popularity
    from scored
    qualify
        row_number() over (
            partition by person_id
            order by
                case gender
                    when 'Male'        then 1
                    when 'Female'      then 2
                    when 'Non-binary'  then 3
                    else 4
                end,
                raw_popularity desc,
                name asc
        ) = 1
)

select * from deduped
