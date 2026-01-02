-- stg_tmdb__cast.sql

with src as (
    select
        movie_id,
        payload_json,
        ingested_at
    from {{ source('tmdb', 'movies')}}
),

-- Handle pagination quirks
deduped as (
    select
        movie_id,
        payload_json as p,
        ingested_at
    from src
    QUALIFY row_number() over (
        partition by movie_id 
        order by ingested_at desc
    ) = 1
),

-- Unnest cast
unnested_cast as (
    select
        movie_id,
        unnest(p.credits.cast) as c,
        ingested_at
    from deduped
)

select
    -- Primary key
    c.credit_id,

    -- Movie grain
    movie_id,
    
    -- Person details
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

    -- Movie specific details
    'Cast' as credit_type,
    c.character,
    c."order" as cast_order,

    -- Auditing
    ingested_at
from unnested_cast

