select
    cast(adult as boolean) as adult,
    backdrop_path,
    cast(genre_ids as jsonb) as genre_ids,
    cast(id as bigint) as movie_id,
    original_language,
    overview,
    cast(popularity as numeric(10,2)) as popularity,
    poster_path,
    cast(release_date as date) as release_date,
    title,
    cast(video as boolean) as video,
    cast(vote_average as numeric(3,1)) as vote_average,
    cast(vote_count as bigint) as vote_count
from
    {{source('raw', 'movies')}}