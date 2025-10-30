-- stg_tmdb__cast.sql
-- Staging model for TMDB cast members
-- Source: TMDB API v3 /movie/{movie_id}/credits endpoint
-- 
-- Transformations applied:
-- 1. Reordered columns for analytical usability
-- 2. Converted empty strings to NULL for proper null handling
-- 3. Applied appropriate data types
-- 4. Mapped gender codes to readable values (1=female, 2=male, 3=non_binary)
-- 5. Renamed 'order' to 'cast_order' to avoid SQL reserved word conflict

SELECT
  -- Primary key
  NULLIF(credit_id, '') AS credit_id,
  
  -- Foreign keys
  NULLIF(movie_id, '')::bigint AS movie_id,
  NULLIF(id, '')::bigint AS person_id,
  
  -- Core identifiers
  NULLIF(name, '') AS name,
  NULLIF(original_name, '') AS original_name,
  
  -- Dimensions
  CASE (gender)::int 
    WHEN 1 THEN 'female' 
    WHEN 2 THEN 'male' 
    WHEN 3 THEN 'non_binary' 
    ELSE 'unknown' 
  END AS gender,
  NULLIF(known_for_department, '') AS known_for_department,
  NULLIF(character, '') AS character,
  
  -- Metrics
  NULLIF(cast_id, '')::int AS cast_id,
  NULLIF("order", '')::int AS cast_order,
  NULLIF(popularity, '')::numeric(12,6) AS popularity,
  
  -- Boolean flags
  NULLIF(adult, '')::boolean AS adult,
  
  -- Media paths
  NULLIF(profile_path, '') AS profile_path

FROM {{ source('raw', 'cast_members') }}