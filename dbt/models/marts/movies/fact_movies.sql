-- fact_movies.sql
with details as (
    select
        movie_id,
        budget,
        popularity,
        revenue,
        runtime,
        vote_average,
        vote_count
    from {{ ref('stg_tmdb__movie_details')  }}
)

select * from details