-- dim_movies.sql
with details as (
    select
        -- PK
        movie_id,
        
        -- Core identifying attributes
        title,
        original_title,
        imdb_id,
        
        -- Descriptive attributes (alphabetical or logical grouping)
        overview,
        tagline,
        
        -- Dates
        release_date,
        
        -- Status/flags
        status,
        adult,
        video,
        
        -- Language/locale
        original_language,
        
        -- Media paths (usually least important)
        poster_path,
        backdrop_path,
        homepage
        
    from {{ ref('stg_tmdb__movie_details') }}
)

select * from details