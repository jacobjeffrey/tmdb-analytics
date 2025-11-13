-- int_tmdb__production_companies.sql
-- Intermediate model: Unique production companies (extracted from stg_tmdb__movie_details )
-- 
-- Grain: One row per company
-- Purpose: Creates person-level dimension from movie-cast relationship
-- Used by: dim_production_companies
--
-- Transformations:
-- - Select and explode production_companies field from movie details model
-- - Select distinct companies, choosing the logo path with the highest alphabetical order

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

select distinct on (company_id, company_name, country_code)
    company_id,
    company_name,
    country_code,
    max(logo_path) as logo_path -- assume the highest value is most recent
from exploded
where company_id is not null
group by company_id, company_name, country_code