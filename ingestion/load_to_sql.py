import psycopg2
import pandas as pd

conn = psycopg2.connect(database = "tmdb",
                        user = "tmdb",
                        host = "localhost",
                        password = "tmdb",
                        port = 5432)