-- top_roi_movies_by_budget_tier_last_5y.sql
with movies as (
  select
    fct.movie_id,
    dim.title,
    dim.release_date,
    fct.revenue_usd,
    fct.budget_usd,
    fct.budget_quality,
    fct.budget_tier,
    safe_divide(fct.revenue_usd, fct.budget_usd) as roi
  from {{ ref('fct_movies') }} as fct
  join {{ ref('dim_movies') }} as dim
    on fct.movie_id = dim.movie_id
  where fct.budget_quality = 'reported'
)

select
  *
from movies
where release_date >= date_sub(current_date, interval 5 year)
qualify row_number() over (
  partition by budget_tier
  order by roi desc
) <= 5
