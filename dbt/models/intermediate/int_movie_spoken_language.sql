-- models/intermediate/movie_spoken_language.sql
with base as (
    select 
        movie_id, production_countries
    from 
        {{ ref('stg_movie_details') }}
)
select
    b.movie_id,
    (pcountry ->> 'name')::text as country_name,
    lower((pcountry ->> 'iso_3166_1'))::char(2) as country_code
from base b 
cross join lateral jsonb_array_elements(b.production_countries) as pcountry
where pcountry ->> 'name' is not null