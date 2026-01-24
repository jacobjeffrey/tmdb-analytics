-- bridge_movies_prod_companies.sql

with source as (
    select
        movie_id,
        company_id
    from {{ ref('int_tmdb_production_companies_unnested') }}
)

select * from source