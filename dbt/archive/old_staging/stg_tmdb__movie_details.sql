-- stg_tmdb__movie_details.sql
SELECT
  -- Primary key
  NULLIF(id, '')::bigint AS movie_id,
  
  -- Foreign keys
  NULLIF(imdb_id, '') AS imdb_id,
  
  -- Core identifiers
  NULLIF(title, '') AS title,
  NULLIF(original_title, '') AS original_title,
  
  -- Important dimensions
  NULLIF(status, '') AS status,
  NULLIF(release_date, '')::date AS release_date,
  LOWER(NULLIF(original_language, '')) AS original_language,
  NULLIF(adult, '')::boolean AS adult,
  
  -- Key metrics
  NULLIF(revenue, '')::bigint AS revenue,
  NULLIF(budget, '')::integer AS budget,
  NULLIF(runtime, '')::integer AS runtime,
  NULLIF(popularity, '')::numeric(12,6) AS popularity,
  NULLIF(vote_average, '')::numeric(4,2) AS vote_average,
  NULLIF(vote_count, '')::bigint AS vote_count,
  
  -- Descriptive content
  NULLIF(overview, '') AS overview,
  NULLIF(tagline, '') AS tagline,
  NULLIF(homepage, '') AS homepage,
  
  -- Complex/JSON fields
  NULLIF(belongs_to_collection, '')::jsonb AS belongs_to_collection,
  NULLIF(genres, '')::jsonb AS genres,
  NULLIF(origin_country, '')::jsonb AS origin_country,
  NULLIF(production_companies, '')::jsonb AS production_companies,
  NULLIF(production_countries, '')::jsonb AS production_countries,
  NULLIF(spoken_languages, '')::jsonb AS spoken_languages,
  
  -- Boolean flags
  NULLIF(video, '')::boolean AS video,
  
  -- Media paths
  NULLIF(backdrop_path, '') AS backdrop_path,
  NULLIF(poster_path, '') AS poster_path

FROM {{ source('raw','movie_details') }}