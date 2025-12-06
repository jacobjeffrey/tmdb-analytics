-- stg_tmdb__genres.sql
-- Staging model for TMDB genre reference data
-- Source: TMDB API v3 /genre/movie/list endpoint
-- 
-- Transformations applied:
-- 1. Converted empty strings to NULL for proper null handling
-- 2. Applied appropriate data types
-- 3. Renamed 'name' to 'genre_name' for clarity

SELECT
  -- Primary key
  NULLIF(id, '')::integer AS genre_id,
  
  -- Identifiers
  NULLIF(name, '')::text AS genre_name

FROM {{ source('raw', 'genres') }}