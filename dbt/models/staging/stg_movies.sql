-- models/staging/stg_movies.sql
SELECT
    NULLIF(adult, '')::boolean                 as adult,
    NULLIF(backdrop_path, '')                  as backdrop_path,
    NULLIF(genre_ids, '')::jsonb               as genre_ids,
    NULLIF(id, '')::bigint                     as movie_id,
    LOWER(NULLIF(original_language, ''))       as original_language,
    NULLIF(overview, '')                       as overview,
    NULLIF(popularity, '')::numeric(12,6)      as popularity,
    NULLIF(poster_path, '')                    as poster_path,
    NULLIF(release_date, '')::date             as release_date,
    NULLIF(title, '')                          as title,
    NULLIF(video, '')::boolean                 as video,
    NULLIF(vote_average, '')::numeric(4,2)     as vote_average,
    NULLIF(vote_count, '')::bigint             as vote_count
FROM
    {{ source('raw', 'movies') }};