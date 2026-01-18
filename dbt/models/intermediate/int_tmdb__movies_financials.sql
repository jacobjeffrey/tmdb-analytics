-- int_tmdb__movies_financials.sql

with movies as (
    select
        movie_id,
        budget_usd,
        revenue_usd,
        budget_quality
    from {{ ref('stg_tmdb__movies') }}
),

thresholds as (
    select
        q1_budget_usd,
        q3_budget_usd
    from {{ ref('int_tmdb__budget_thresholds') }}
)

select
    movies.movie_id,
    movies.budget_usd,
    movies.revenue_usd,
    movies.budget_quality,
    case
        when movies.budget_usd is null
            or movies.budget_usd < 1000
            or movies.budget_quality != 'reported'
            then null
        when movies.budget_usd <= thresholds.q1_budget_usd then 'low'
        when movies.budget_usd <= thresholds.q3_budget_usd then 'mid'
        else 'high'
    end as budget_tier
from movies
cross join thresholds
