# Initial steps
The purpose of this project to try and predict box office prediction using a variety
of metrics. We will use features such as the "star power" of the main cast, the director,
the studio, etc. Later iterations of the project will attempt to use NLP, namely 
sentiment analysis, to further improve predictions

# Potential data sources
**The Movie Database (https://www.themoviedb.org/)**
- free API
- movies endpoint: adult (boolean), budget,  revenue, keywords, production companies, release_date, tagline

things i've learned:
- how to pull info from APIs using requests
- APIs often have a page limit, need to cap that
- use data = session.get(), data.get('key') to check if a key exists