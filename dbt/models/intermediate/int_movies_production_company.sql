-- models/intermediate/movies_production_company.sql
with base as (
    select 
        movie_id, production_companies 
    from 
        {{ ref('stg_movie_details') }}
)
select
    b.movie_id,
    jsonb_extract_path_text(pc, 'id')::bigint as company_id
from base b 
cross join lateral jsonb_array_elements(b.production_companies) as pc
where jsonb_extract_path_text(pc, 'id') is not null