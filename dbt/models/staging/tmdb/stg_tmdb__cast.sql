-- stg_tmdb__cast.sql
select
    NULLIF(adult, '')::boolean as adult,
    CASE (gender)::int
        WHEN 1 THEN 'female'
        WHEN 2 THEN 'male'
        WHEN 3 THEN 'non_binary'
        ELSE 'unknown'
    END AS gender,
    NULLIF(id, '')::bigint as person_id,
    NULLIF(known_for_department, '') as known_for_department,
    NULLIF(name, '') as name,
    NULLIF(original_name, '') as original_name,
    NULLIF(popularity, '')::numeric(12,6) as popularity,
    NULLIF(profile_path, '') as profile_path,
    NULLIF(cast_id, '')::int as cast_id,
    NULLIF(character, '') as character,
    NULLIF(credit_id, '') as credit_id,
    NULLIF("order", '')::int as cast_order,
    NULLIF(movie_id, '')::bigint as movie_id
from
    {{ source('raw', 'cast_members') }}