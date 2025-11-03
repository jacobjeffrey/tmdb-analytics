-- stg_tmdb__production_companies.sql
-- Intermediate model: Unique production companies (extracted from stg_tmdb__movie_details )
-- 
-- Grain: One row per company
-- Purpose: Creates person-level dimension from movie-cast relationship
-- Used by: dim_production_companies
--
-- Transformations:
-- - Select and explode production_companies field from movie details model
-- - Select distinct companies

with src as (
    select production_companies::jsonb from {{ ref('stg_tmdb__movie_details') }}
),

exploded as (
    select
        (pc ->> 'id')::bigint as company_id,
        (pc ->> 'logo_path')::text as logo_path,
        (pc ->> 'name')::text as company_name,
        nullif(upper((pc ->> 'origin_country')),'')::char(2) as country_code
    from src
    cross join lateral jsonb_array_elements(src.production_companies) as pc
)

select distinct
    company_id,
    logo_path,
    company_name,
    country_code
from exploded
where company_id is not null