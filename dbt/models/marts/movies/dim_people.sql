-- dim_actors.sql
select * from {{ ref('stg_tmdb__people')  }}