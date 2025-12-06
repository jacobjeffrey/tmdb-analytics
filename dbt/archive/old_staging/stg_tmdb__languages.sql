-- stg_tmdb__languages.sql
-- Staging model for TMDB language reference data
-- Source: TMDB API v3 /configuration/languages endpoint
-- 
-- Transformations applied:
-- 1. Standardized language codes to lowercase ISO 639-1 format
-- 2. Converted empty strings to NULL for proper null handling
-- 3. Applied appropriate data types
-- 4. Renamed 'name' to 'native_name' for clarity

SELECT
  -- Primary key
  NULLIF(LOWER(iso_639_1), '')::char(2) AS language_code,
  
  -- Identifiers
  NULLIF(english_name, '')::text AS english_name,
  NULLIF(name, '')::text AS native_name

FROM {{ source('raw', 'languages') }}