# Data Ingestion
In this step, I explored the potential data sources I could use for my project.

## Data Sources Examined
### The Movie Database (https://www.themoviedb.org/) ###
At least for the initial step, I decided to use data solely from The Movie Database (TMDB).
They offer a free API with a generous rate limit (50/s) and it's quite comprehensive with multiple endpoints.
I decided to use the following endpoints:
- discover/movie: basic movie information, such as language, genres, overview, popularity, etc
- movie/details: more details about the movies, such as production companies, budget, revenue
- genre/movie: a list of genres and their ids
- movie/{id}/credits: an endpoint that provides both the cast and crew of a movie. for my project, i kept only the cast

### Letterboxd (https://letterboxd.com/) ###
After pulling my data from TMDB, I decided not to pull anything from Letterboxd for now.
However, for future features I may download reviews and genres, which are more detailed than TMDB's.
Based on what I saw in their documentation and robots.txt, it seems scraping from individual movie pages is okay.
However, they disallow scraping from certain links such as /*/by or /popular/this/*.

### RateYourMusic and IMDB ###
These are out of the question. RateYourMusic's robot.txt explicitly prohibits all scraping.
And IMDB also discourages it, instead pointing you towards their official API which is costly for a portfolio project.

## Initial Data Pull
For the initial data pull, I created simple synchronous scripts that processed.
to discuss: 
- discover/movies caps each request at 500 pages, needed to filter low popularity ones out
- fetched movies from 2000 to 2024
- movie/details and cast needed to be pulled individually for each movie
- genres returns a short list
- learned about serializing json, using .get() to safely fetch keys

## Second Data Pull
to discuss:
- wanted to do EDA on downloaded files and prepare for SQL upload
- inspection of movies.csv revealed improperly terminated lines, mostly likely due to unicode characters in fields like 'summary' or 'tagline'
- needed to redownload the data, this time using quoting=csv.QUOTE_ALL
- because of need to increase throughput, decided to refactor scripts to be asynchronous
- added utils.py
- respected limit by using aiolimiter and semaphore

## Summary
to discuss:
- recap of things learned and files created
- next step
- potential future projects (ex. NLP integration with summary/tagline, review/detailed genre data from letterboxd)