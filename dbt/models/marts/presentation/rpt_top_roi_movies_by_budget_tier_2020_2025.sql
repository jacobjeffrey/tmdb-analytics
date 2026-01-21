-- rpt_top_roi_movies_by_budget_tier_2020_to_2025.sql
with movies as (
  select
    fct.movie_id,
    dim.title,
    dim.release_date,
    fct.revenue_usd,
    fct.budget_usd,
    fct.budget_quality,
    fct.budget_tier,
    fct.revenue_usd/nullif(fct.budget_usd, 0) as roi
  from {{ ref('fct_movies') }} as fct
  join {{ ref('dim_movies') }} as dim
    on fct.movie_id = dim.movie_id
  where fct.budget_quality = 'reported'
)

select
  *
from movies
where release_date >= date '2020-01-01'
  and release_date < date '2026-01-01'
qualify row_number() over (
  partition by budget_tier
  order by roi desc
) <= 5
