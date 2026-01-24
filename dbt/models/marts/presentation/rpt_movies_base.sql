-- rpt_movies_base.sql

select
    d.movie_id,
    d.title,
    d.original_title,
    d.status,
    d.release_date,
    d.original_language,
    f.runtime,
    f.revenue_usd,
    f.budget_usd,
    nullif(f.revenue_usd,0)/nullif(f.budget_usd,0) as roi,
    f.budget_quality,
    f.budget_tier
from {{ ref('dim_movies') }} as d
join {{ ref('fct_movies') }} as f
    on d.movie_id = f.movie_id