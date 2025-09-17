-- models/staging/stg_cast.sql
select
    NULLIF(adult, '')::boolean               as adult,
    NULLIF(gender, '')::int                  as gender,
    NULLIF(id, '')::bigint                   as person_id,
    NULLIF(known_for_department, '')         as known_for_department,
    NULLIF(name, '')                         as name,
    NULLIF(original_name, '')                as original_name,
    NULLIF(popularity, '')::numeric(12,6)    as popularity,
    NULLIF(profile_path, '')                 as profile_path,
    NULLIF(cast_id, '')::int                 as cast_id,
    NULLIF(character, '')                    as character,
    NULLIF(credit_id, '')                    as credit_id,
    NULLIF("order", '')::int                 as cast_order,
    NULLIF(movie_id, '')::bigint             as movie_id
from
    {{ source('raw', 'cast_members') }}