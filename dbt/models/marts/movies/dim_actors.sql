-- dim_actors.sql
with cast_unique as (
    select * from {{ ref('int_cast_unique')  }}
)

select * from cast_unique