-- dim_production_companies.sql
with companies as (
    select * from {{ ref('stg_tmdb__production_companies')  }}
)

select * from companies