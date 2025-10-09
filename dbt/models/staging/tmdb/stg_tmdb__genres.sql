-- stg__tmdb__genres.sql
SELECT
    NULLIF(id, '')::integer as genre_id,
    NULLIF(name, '')::text as genre_name
FROM
    {{ source('raw', 'genres') }}