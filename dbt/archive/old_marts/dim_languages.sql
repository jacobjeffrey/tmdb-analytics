-- dim_languages.sql
with languages as (
    select * from {{ ref('stg_tmdb__languages')  }}
)

select * from languages