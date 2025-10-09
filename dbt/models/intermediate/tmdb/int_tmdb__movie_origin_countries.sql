-- int_tmdb__movie_origin_countries.sql
with base as (
    select 
        movie_id, origin_country 
    from 
        {{ ref('stg_tmdb__movie_details') }}
)
select
    b.movie_id,
    trim(both '"' from oc.country_code)::char(2) as country_code
from base b
cross join lateral jsonb_array_elements_text(b.origin_country) as oc(country_code)
where trim(both '"' from oc.country_code) <> ''