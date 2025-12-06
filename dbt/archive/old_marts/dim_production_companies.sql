-- dim_production_companies.sql
-- Dim table for production companies (derived from int_movies_production_companies_exploded)
--
-- Grain: One row for each production_company
--
-- Transformations:
-- - Dedupe the int_movies_production_companies_exploded model
-- - Select max logo_path (assumes larger one is most recent)


with companies as (
    select * from {{ ref('int_movies_production_companies_exploded')  }}
)

select distinct
    company_id,
    company_name,
    country_code,
    max(logo_path) as logo_path -- choose the most recent one
from companies
group by company_id, company_name, country_code