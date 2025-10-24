-- bridge_movies_production_companies.sql
with movies_production_companies as (
    select * from {{ ref('int_movies_production_companies') }}
)

select * from movies_production_companies