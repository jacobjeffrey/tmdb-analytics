-- stg_production_companies.sql
with src as (
    select production_companies::jsonb from {{ source('raw', 'movie_details')}}
),
exploded as (
    select
        jsonb_extract_path_text(pc, 'id')::bigint as company_id,
        jsonb_extract_path_text(pc, 'logo_path')::text as logo_path,
        jsonb_extract_path_text(pc, 'name')::text as company_name,
        jsonb_extract_path_text(pc, 'origin_country')::char(2) as country_code
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