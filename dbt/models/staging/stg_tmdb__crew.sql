-- models/staging/stg_tmdb__crew.sql

with src as (
    select
        movie_id,
        payload_json,
        ingested_at
    from {{ source('tmdb', 'movies') }}
),

-- Handle pagination quirks
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

-- Unnest crew (BigQuery: UNNEST in FROM; DuckDB->BQ wrapper uses .list + .element)
unnested_crew as (
    select
        d.movie_id,
        c_row.element as c,
        d.ingested_at
    from deduped d
    cross join unnest(d.p.credits.crew.list) as c_row
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
    'Crew' as credit_type,
    c.job,
    c.department,

    -- Auditing
    ingested_at
from unnested_crew
