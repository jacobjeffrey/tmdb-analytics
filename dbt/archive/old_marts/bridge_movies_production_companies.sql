-- bridge_movies_production_companies.sql
-- Bridge table for movies and productions companies (generated with int_movies_production_companies_exploded)
--
-- Grain: One entry for each movie_id and company_id combo
--

with base as (
    select movie_id, company_id
    from {{ ref('int_movies_production_companies_exploded') }}
)

select * from base