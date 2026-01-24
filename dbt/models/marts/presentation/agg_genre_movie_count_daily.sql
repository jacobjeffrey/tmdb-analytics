-- agg_genre_movie_count_daily.sql

with recent_movies as (
  select
    movie_id,
    release_date
  from {{ ref('dim_movies') }}
  where release_date is not null
),

movies_by_genre as (
  select
    rm.release_date,
    b.genre_id,
    b.movie_id
  from recent_movies rm
  join {{ ref('bridge_movies_genres') }} b
    on b.movie_id = rm.movie_id
)

select
  mbg.release_date,
  g.genre_id,
  g.name as genre_name,
  count(distinct mbg.movie_id) as movie_count
from movies_by_genre mbg
join {{ ref('dim_genres') }} g
  on mbg.genre_id = g.genre_id
group by
  mbg.release_date,
  g.genre_id,
  g.name
