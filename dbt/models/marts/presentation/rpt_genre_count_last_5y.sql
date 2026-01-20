-- genre_count_last_5y.sql
with recent_movies as (
  select
    movie_id,
    extract(year from release_date) as release_year
  from {{ ref('dim_movies') }}
  where release_date >= date_sub(current_date, interval 5 year)
),

movies_by_genre as (
  select
    bridge.genre_id,
    bridge.movie_id,
    recent_movies.release_year
  from {{ ref('bridge_movies_genres') }} as bridge
  join recent_movies
    on bridge.movie_id = recent_movies.movie_id
)

select
  movies_by_genre.release_year,
  genres.genre_id,
  genres.name as genre_name,
  count(distinct movies_by_genre.movie_id) as movie_count_last_5y
from movies_by_genre
join {{ ref('dim_genres') }} as genres
  on movies_by_genre.genre_id = genres.genre_id
group by
  movies_by_genre.release_year,
  genres.genre_id,
  genres.name
