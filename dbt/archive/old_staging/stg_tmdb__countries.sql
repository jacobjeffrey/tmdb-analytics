-- stg_tmdb__countries.sql
-- Staging model for TMDB country reference data
-- Source: TMDB API v3 /configuration/countries endpoint
-- 
-- Transformations applied:
-- 1. Standardized country codes to uppercase ISO 3166-1 alpha-2 format
-- 2. Converted empty strings to NULL for proper null handling
-- 3. Applied appropriate data types

SELECT
  -- Primary key
  NULLIF(UPPER(iso_3166_1), '')::char(2) AS country_code,
  
  -- Identifiers
  NULLIF(english_name, '')::text AS english_name,
  NULLIF(native_name, '')::text AS native_name

FROM {{ source('raw', 'countries') }}