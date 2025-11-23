-- stg_tmdb__production_companies.sql
-- Staging model for TMDB production companies (dimension table)
-- Source: Extracted from production_companies JSONB array in movie_details
-- 
-- Transformations applied:
-- 1. Unnested JSONB array to create one row per company
-- 2. Deduplicated companies appearing in multiple movies
-- 3. Standardized country codes to uppercase ISO 3166-1 alpha-2 format
-- 4. Converted empty strings to NULL for proper null handling

with src as (
    select production_companies::jsonb from {{ source('raw', 'movie_details')}}
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