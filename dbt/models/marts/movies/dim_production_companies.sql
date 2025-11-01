-- dim_production_companies.sql
with companies as (
    select * from {{ ref('int_production_companies')  }}
)

select * from companies