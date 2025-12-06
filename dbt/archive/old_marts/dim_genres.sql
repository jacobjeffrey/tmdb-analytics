-- dim_genres.sql
with genres as (
    select * from {{ ref('stg_tmdb__genres')  }}
)

select * from genres