-- fact_movies.sql

with details as (
    select
        -- Grain key
        movie_id,

        -- Financial metrics
        budget,
        revenue,

        -- Engagement / popularity metrics
        popularity,

        -- Content / operational metrics
        runtime,

        -- Rating metrics
        vote_average,
        vote_count
    from {{ ref('stg_tmdb__movie_details') }}
)

select * from details;
