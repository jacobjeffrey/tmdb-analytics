-- int_tmdb_movies_origin_country

with source as (
    select
        movie_id,
        origin_country
    from {{ ref('stg_tmdb__movies') }}
),

unnested as (
    select
        s.movie_id,
        oc.element as origin_country_code
    from source as s
    cross join unnest(ifnull(s.origin_country.list, [])) as oc
)

select
    movie_id,
    origin_country_code
from unnested
