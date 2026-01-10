-- models/staging/stg_tmdb__cast.sql

with src as (
    select
        movie_id,
        payload_json,
        ingested_at
    from {{ source('tmdb', 'movies') }}
),

deduped as (
    select
        movie_id,
        payload_json as p,
        ingested_at
    from src
    qualify row_number() over (
        partition by movie_id
        order by ingested_at desc
    ) = 1
),

unnested_cast as (
    select
        d.movie_id,
        c_row.element as c,
        d.ingested_at
    from deduped d
    cross join unnest(d.p.credits.cast.list) as c_row
)

select
    c.credit_id,
    movie_id,

    c.id as person_id,
    c.name,
    c.original_name,
    case
        when c.gender = 1 then 'Female'
        when c.gender = 2 then 'Male'
        when c.gender = 3 then 'Non-binary'
        else 'Not specified'
    end as gender,
    c.known_for_department,
    c.popularity,

    'Cast' as credit_type,
    c.character,
    c.`order` as cast_order,

    ingested_at
from unnested_cast
