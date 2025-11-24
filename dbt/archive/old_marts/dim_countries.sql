-- dim_countries.sql
with countries as (
    select * from {{ ref('stg_tmdb__countries')  }}
)

select * from countries