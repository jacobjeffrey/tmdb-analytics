# Initial steps
The initial purpose of this project was to use TMDB data to create box office prediction models.  
However, I figured I should probably get the fundamentals of data analysis down first, and so I decided to focus on SQL modeling and EDA for now.  
I may come back to the ML ideas at a future time.

# Potential data sources
### The Movie Database (https://www.themoviedb.org/) ###
- Free API
- Movies endpoint: adult (boolean), budget,  revenue, keywords, production companies, release_date, tagline
- Pulled in movies from 2000 to 2024, vote_count > 10
- Pulled in genres
- the popular list for people pulls in too many pages, need to figure out how to pull them in with movie data, may need to learn caching

### Letterboxd ###
- Need to check if scrape friendly
- According to their robot.txt, a lot of their main pages (e.g. most popular films) are off-limits, but I can probably scrape from the individual movie pages
- So far, I might just need their subgenres for future analyses

# Things I've Learned #
- How to pull info from APIs using requests
- APIs often have a page limit, need to cap that
- Use data = session.get(), data.get('key') to check if a key exists
- Trying to parse the .json as is can lead to issues if one of the fields consists of lists or dicts.

# Database setup
With the data files now downloaded, I will perform a quick EDA to check out if there's missing values, duplicates, etc.
Once that's done, I will start the schema and database creation with dbt