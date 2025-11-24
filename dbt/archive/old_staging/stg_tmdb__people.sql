-- stg_tmdb__people.sql
-- Staging model for TMDB people/person details
-- Source: TMDB API v3 /person/{person_id} endpoint
--
-- Transformations applied:
-- 1. Reordered columns for analytical usability
-- 2. Converted empty strings to NULL for proper null handling
-- 3. Applied appropriate data types
-- 4. Mapped gender codes to readable values (1=female, 2=male, 3=non_binary)
-- 5. Parsed also_known_as array field
-- 6. Converted date strings to proper date types

SELECT
    -- Primary key
    NULLIF(id, '')::bigint AS person_id,
    
    -- External identifiers
    NULLIF(imdb_id, '') AS imdb_id,
    
    -- Core identifiers
    NULLIF(name, '') AS name,
    NULLIF(also_known_as, '') AS also_known_as, -- JSON array field
    
    -- Dimensions
    CASE (gender)::int
        WHEN 1 THEN 'female'
        WHEN 2 THEN 'male'
        WHEN 3 THEN 'non_binary'
        ELSE 'unknown'
    END AS gender,
    NULLIF(known_for_department, '') AS known_for_department,
    NULLIF(place_of_birth, '') AS place_of_birth,
    
    -- Biographical info
    NULLIF(biography, '') AS biography,
    NULLIF(birthday, '')::date AS birthday,
    NULLIF(deathday, '')::date AS deathday,
    
    -- Metrics
    NULLIF(popularity, '')::numeric(12,6) AS popularity,
    
    -- Boolean flags
    NULLIF(adult, '')::boolean AS adult,
    
    -- Media paths
    NULLIF(profile_path, '') AS profile_path,
    NULLIF(homepage, '') AS homepage

FROM {{ source('raw', 'people') }}