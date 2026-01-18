-- fct_movies.sql

with movies as (
    select
        movie_id,
        runtime,
        popularity,
        vote_average,
        vote_count
    from {{ ref('stg_tmdb__movies' )}}
),

financials as (
    select
        movie_id,
        revenue_usd,
        budget_usd,
        budget_quality,
        budget_tier
    from {{ ref('int_tmdb__movies_financials') }}
)

select
    movies.movie_id,
    financials.revenue_usd,
    financials.budget_usd,
    financials.budget_quality,
    financials.budget_tier,
    movies.runtime,
    movies.popularity,
    movies.vote_average,
    movies.vote_count
from movies
left join financials
    on movies.movie_id = financials.movie_id
