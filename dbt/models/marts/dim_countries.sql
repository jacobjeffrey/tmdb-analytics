-- dim_countries.sql

with base as (
    select 
        iso_3166_1,
        english_name,
        native_name
    from {{ ref('countries') }}
)

select
    iso_3166_1 as country_code,
    english_name,
    native_name
from base