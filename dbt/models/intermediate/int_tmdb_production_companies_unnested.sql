-- int_tmdb_production_companies_unnested
with src as (
    select 
        movie_id, 
        production_companies, 
        ingested_at
    from {{ ref('stg_tmdb__movies')}}
),

exploded as (
    select
        movie_id,
        pc.id as company_id,
        pc.name as company_name,
        pc.origin_country,
        pc.logo_path,
        ingested_at
    from src,
    unnest(production_companies) as t(pc)
),

deduped as (
    select *
    from exploded
    qualify row_number() over (
        partition by movie_id, company_id
        order by ingested_at
    ) = 1
)

select
    movie_id,
    company_id,
    company_name,
    case
      when origin_country in ('', ' ') then null
      else upper(left(origin_country, 2))
    end as origin_country,
    logo_path
from deduped