-- bridge_movies_production_countries.sql
with movies_production_countries as (
    select * from {{ ref('int_movies_production_countries') }}
)

select * from movies_production_countries