-- marts/core/dim_date.sql

with bounds as (
  select
    coalesce(min(release_date), date '1970-01-01') as min_date,
    greatest(
      coalesce(max(release_date), current_date()),
      date_add(current_date(), interval 1 year)
    ) as max_date
  from {{ ref('dim_movies') }}
),

spine as (
  select
    d as date_day
  from bounds
  cross join unnest(
    generate_date_array(
      bounds.min_date,
      bounds.max_date
    )
  ) as d
),

date_parts as (
  select
    date_day,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    extract(week from date_day) as week,
    extract(dayofweek from date_day) as day_of_week, -- Sunday = 1
    extract(isoweek from date_day) as iso_week,
    date_trunc(date_day, week) as week_start_date,
    date_trunc(date_day, month) as month_start_date
  from spine
)

select
  date_day,

  -- calendar
  year,
  quarter,
  month,
  format_date('%B', date_day) as month_name,
  week,
  iso_week,

  -- weekday
  day_of_week,
  format_date('%A', date_day) as day_name,
  day_of_week in (1, 7) as is_weekend,

  -- convenience fields
  format_date('%Y-%m', date_day) as year_month,
  week_start_date,
  month_start_date,

  -- flags for dashboards
  week_start_date = date_trunc(current_date(), week) as is_current_week,
  month_start_date = date_trunc(current_date(), month) as is_current_month

from date_parts
