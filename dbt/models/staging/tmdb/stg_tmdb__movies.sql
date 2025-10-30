-- stg_tmdb__movies.sql
-- Staging model for TMDB movie list data
-- Source: TMDB API v3 /discover/movie or /movie/popular endpoints
-- 
-- Transformations applied:
-- 1. Reordered columns for analytical usability
-- 2. Converted empty strings to NULL for proper null handling
-- 3. Applied appropriate data types
-- 4. Standardized language codes to lowercase

SELECT
  -- Primary key
  NULLIF(id, '')::bigint AS movie_id,
  
  -- Core identifiers
  NULLIF(title, '') AS title,
  
  -- Important dimensions
  LOWER(NULLIF(original_language, '')) AS original_language,
  NULLIF(release_date, '')::date AS release_date,
  
  -- Key metrics
  NULLIF(popularity, '')::numeric(12,6) AS popularity,
  NULLIF(vote_average, '')::numeric(4,2) AS vote_average,
  NULLIF(vote_count, '')::bigint AS vote_count,
  
  -- Descriptive content
  NULLIF(overview, '') AS overview,
  
  -- Complex/JSON fields
  NULLIF(genre_ids, '')::jsonb AS genre_ids,
  
  -- Boolean flags
  NULLIF(adult, '')::boolean AS adult,
  NULLIF(video, '')::boolean AS video,
  
  -- Media paths
  NULLIF(backdrop_path, '') AS backdrop_path,
  NULLIF(poster_path, '') AS poster_path

FROM {{ source('raw', 'movies') }}