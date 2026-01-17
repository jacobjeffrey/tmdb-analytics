-- int_tmdb__budget_thresholds.sql

select
    percentile_cont(budget_usd, 0.25) as q1_budget_usd,
    percentile_cont(budget_usd, 0.75) as q3_budget_usd,
    count(*) as n_budgets_used
from {{ ref('stg_tmdb__movies') }}
where budget_usd is not null
  and budget_usd >= 1000
  and budget_quality = 'reported'
