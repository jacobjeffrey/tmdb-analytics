-- dim_languages.sql

with base as (
    select
        iso_639_1,
        english_name,
        name
    from {{ ref('languages') }}
)

select
    iso_639_1 as language_code,
    english_name,
    name as native_name
from base