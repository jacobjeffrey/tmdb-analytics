-- dim_people.sql

with base as (
    select
        person_id,
        name,
        original_name,
        gender,
        known_for_department,
        popularity
    from {{ ref('int_tmdb_cast_crew_combined') }}
),

deduped as (
    select
        person_id,
        name,
        original_name,
        gender,
        known_for_department,

        -- average popularity across duplicates for the same person_id
        round(
            avg(popularity) over (partition by person_id),
            3
        ) as popularity

    from base

    -- pick the “best” row per person_id using a gender priority + popularity
    qualify
        row_number() over (
            partition by person_id
            order by
                case gender
                    when 'Male'        then 1
                    when 'Female'      then 2
                    when 'Non-binary'  then 3
                    else 4  -- Not specified / null
                end,
                popularity desc,
                name asc
        ) = 1
)

select * from deduped