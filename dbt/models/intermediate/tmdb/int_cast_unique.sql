-- int_tmdb__cast_unique.sql
-- Intermediate model: Unique cast members (deduped from movie-level cast data)
-- 
-- Grain: One row per person
-- Purpose: Creates person-level dimension from movie-cast relationship
-- Used by: dim_cast_members
--
-- Transformations:
-- - Deduplicates actors appearing in multiple movies
-- - Drops movie-specific attributes (character, cast_order, credit_id)

WITH base AS (
    SELECT * FROM {{ ref('stg_tmdb__cast') }}
),

person_attributes AS (
    SELECT
        -- Primary key
        person_id,
        
        -- Identifiers
        name,
        original_name,
        
        -- Dimensions
        gender,
        known_for_department,
        
        -- Metrics
        popularity,
        
        -- Media
        profile_path
    FROM base
)

SELECT DISTINCT *
FROM person_attributes