with details as (
    select
        movie_id,
        imdb_id,

        title,
        original_title,
        original_language,

        overview,
        tagline,

        release_date,
        status,

        adult,
        video,

        backdrop_path,
        poster_path,
        homepage,

        belongs_to_collection,
        genres,
        production_companies,
        production_countries,
        spoken_languages,
        origin_country
    from {{ ref('stg_tmdb__movie_details') }}
)

select * from details;
