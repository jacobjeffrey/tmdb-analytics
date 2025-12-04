-- dim_companies.sql

with base as (
    select
        company_id,
        company_name,
        origin_country,
        logo_path
    from {{ ref('int_tmdb_production_companies_unnested')}}
),

deduped as (
    select *
    from base
    qualify row_number() over (
        partition by company_id
        order by logo_path desc -- assume highest value is most recent logo
    ) = 1
)

select
    company_id,
    company_name,
    origin_country,
    logo_path
from deduped
order by company_id