-- int_tmdb__movie_genres.sql
with base as (
    select * from {{ ref('stg_tmdb__cast') }}
),
selected_columns as (
    select
        gender,
        person_id,
        known_for_department,
        name,
        original_name,
        popularity,
        profile_path
    from base
)

select distinct *
from selected_columns

