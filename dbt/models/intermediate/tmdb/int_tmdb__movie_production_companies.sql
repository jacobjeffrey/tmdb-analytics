-- int_tmdb__movie_production_companies.sql
with base as (
    select 
        movie_id, production_companies 
    from 
        {{ ref('stg_tmdb__movie_details') }}
)
select
    b.movie_id,
    jsonb_extract_path_text(pc, 'id')::bigint as company_id
from base b 
cross join lateral jsonb_array_elements(b.production_companies) as pc
where jsonb_extract_path_text(pc, 'id') is not null