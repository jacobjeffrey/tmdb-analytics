-- bridge_movies_origin_countries.sql
with movies_origin_countries as (
    select * from {{ ref('int_movies_origin_countries') }}
)

select * from movies_origin_countries