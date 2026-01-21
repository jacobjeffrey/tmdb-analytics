-- rpt_movie_data_completeness_2020_2025.sql

with recent_movies as (
    select
        d.movie_id,
        f.budget_quality,
        f.revenue_usd,
        f.runtime
    from {{ ref('dim_movies') }} as d
    join {{ ref('fct_movies')}} as f
        on d.movie_id = f.movie_id
    where
        d.release_date >= date '2020-01-01' and
        d.release_date < date '2026-01-01'
)

select
    100.0 * sum(case when budget_quality = 'reported' then 1 else 0 end)/count(*) 
        as pct_budget_reported,
    100.0 * sum(case when revenue_usd is not null then 1 else 0 end)/count(*) 
        as pct_revenue_reported,
    100.0 * sum(case when runtime is not null then 1 else 0 end)/count(*) 
        as pct_runtime_reported,
from recent_movies