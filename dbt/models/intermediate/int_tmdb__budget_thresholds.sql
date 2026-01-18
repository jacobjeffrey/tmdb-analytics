-- int_tmdb__budget_thresholds.sql

with valid as (
  select budget_usd
  from {{ ref('stg_tmdb__movies') }}
  where budget_usd is not null
    and budget_usd >= 1000
    and budget_quality = 'reported'
),
q as (
  select approx_quantiles(budget_usd, 4) as qs
  from valid
),
n as (
  select count(*) as n_budgets_used
  from valid
)
select
  q.qs[offset(1)] as q1_budget_usd,
  q.qs[offset(3)] as q3_budget_usd,
  n.n_budgets_used
from q
cross join n
