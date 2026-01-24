-- dim_genres.sql

with base as (
    select
        id,
        name
    from {{ ref('genres') }}
)

select
    id as genre_id,
    name
from base