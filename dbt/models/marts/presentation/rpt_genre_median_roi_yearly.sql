-- rpt_genre_median_roi_yearly.sql

with movie_base as (
    select
        movie_id,
        extract(year from release_date) as release_year,
        roi
    from {{ ref('rpt_movies_base' )}}
    where budget_quality = 'reported'
),

genre_fanout as (
    select
        m.movie_id,
        m.release_year,
        m.roi,
        g.genre_id,
        g.name as genre_name
    from movie_base as m
    join {{ ref('bridge_movies_genres' )}} as b
        on m.movie_id = b.movie_id
    join {{ ref('dim_genres') }} as g
        on g.genre_id = b.genre_id
),

roi_calculation as (
    select
        genre_name,
        release_year,
        approx_quantiles(roi, 100)[offset(50)] as median_roi
    from genre_fanout
    group by 1,2
)

select * from roi_calculation
order by release_year desc, median_roi desc