-- bridge_movies_spoken_languages.sql
with movies_languages as (
    select * from {{ ref('int_movies_spoken_languages') }}
)

select * from movies_languages