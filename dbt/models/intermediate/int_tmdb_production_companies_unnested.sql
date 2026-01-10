-- int_tmdb_production_companies_unnested

with src as (
    select
        movie_id,
        production_companies,
        ingested_at
    from {{ ref('stg_tmdb__movies') }}
),

exploded as (
    select
        s.movie_id,
        pc.element.id            as company_id,
        pc.element.name          as company_name,
        pc.element.origin_country as origin_country,
        pc.element.logo_path     as logo_path,
        s.ingested_at
    from src as s
    cross join unnest(ifnull(s.production_companies.list, [])) as pc
),

deduped as (
    select *
    from exploded
    qualify row_number() over (
        partition by movie_id, company_id
        order by ingested_at desc
    ) = 1
)

select
    movie_id,
    company_id,
    company_name,
    case
      when origin_country is null then null
      when trim(origin_country) = '' then null
      else upper(substr(origin_country, 1, 2))
    end as origin_country,
    logo_path
from deduped
