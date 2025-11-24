-- int_tmdb__production_companies_exploded.sql
-- Intermediate model: Movies with production_company field exploded.
-- 
-- Grain: One row per movie_id and company pairing
-- Purpose: Unnest and list all movie/production company pairings
-- Used by: dim_production_companies, bridge_movies_production_companies
--
-- Transformations:
-- - Explode production_companies JSON from stg_tmdb__movie_details
-- - Select distinct over all columns

with src as (
    select movie_id, production_companies::jsonb from {{ ref('stg_tmdb__movie_details') }}
),

exploded as (
    select
        movie_id,
        (pc ->> 'id')::bigint as company_id,
        (pc ->> 'logo_path')::text as logo_path,
        (pc ->> 'name')::text as company_name,
        nullif(upper((pc ->> 'origin_country')),'')::char(2) as country_code
    from src
    cross join lateral jsonb_array_elements(src.production_companies) as pc
)

-- sometimes tmdb has a duplicate company, need to dedupe
select distinct
    movie_id,
    company_id,
    company_name,
    country_code,
    logo_path
from exploded