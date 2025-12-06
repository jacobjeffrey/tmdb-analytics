-- dim_movies.sql

with details as (
    select
        -- Grain key
        movie_id,
        imdb_id,

        -- Titles & naming
        title,
        original_title,
        original_language,

        -- Descriptive metadata
        overview,
        tagline,

        -- Release status metadata
        release_date,
        status,

        -- Content flags
        adult,
        video,

        -- Media asset paths
        backdrop_path,
        poster_path,
        homepage,

        -- Collections & categorical attributes
        belongs_to_collection,
        genres,
        production_countries,
        spoken_languages,
        origin_country

    from {{ ref('stg_tmdb__movie_details') }}
)

select * from details