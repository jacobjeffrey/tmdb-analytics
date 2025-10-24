-- dim_movies.sql
with details as (
    select
        is_adult,
        backdrop_path,
        homepage,
        movie_id,
        imdb_id,
        original_language,
        original_title,
        overview,
        poster_path,
        release_date,
        status,
        tagline,
        title,
        is_video
    from {{ ref('stg_tmdb__movie_details')  }}
)

select * from details