from Connection import Connection
from TablesETL.Awards import Awards
from TablesETL.Category import Category
from TablesETL.FilmCountries import FilmCountries
from Webscraping import Webscraping
from TablesETL.Film import Film
from TablesETL.FilmDirectors import FilmDirectors
from TablesETL.Director import Director
from TablesETL.FilmProtagonists import FilmProtagonists
from TablesETL.Protagonist import Protagonist
from TablesETL.Country import Country

connection = Connection()

with open('scripts/snowflake_schema.sql', 'r') as file:
    for line in file.read().split(';'):
        connection.execute(line)

web_scrap = Webscraping()
web_scrap.start_scrape()

df_initial = web_scrap.get_df_initial()
df_durations = web_scrap.get_df_durations()

df_protagonists = web_scrap.get_df_protagonists()
df_directors = web_scrap.get_df_directors()
df_countries = web_scrap.get_df_countries()
df_awards = web_scrap.get_df_awards()

df_protagonists_copy = df_protagonists.copy()
df_directors_copy = df_directors.copy()
df_countries_copy = df_countries.copy()
df_awards_copy = df_awards.copy()

# Films ETL

film_etl = Film(connection)
film_etl.start_etl(df_initial, df_durations)

# Directors ETL

film_director_etl = FilmDirectors(connection)
director_etl = Director(connection)

director_etl.start_etl(df_directors)
film_director_etl.start_etl(df_directors_copy, director_etl.get_dataframe())

# Protagonists ETL


film_protagonist_etl = FilmProtagonists(connection)
protagonist_etl = Protagonist(connection)

protagonist_etl.start_etl(df_protagonists)
film_protagonist_etl.start_etl(df_protagonists_copy, protagonist_etl.get_dataframe())

# Countries ETL


film_country_etl = FilmCountries(connection)
country_etl = Country(connection)

country_etl.start_etl(df_countries)
film_country_etl.start_etl(df_countries_copy, country_etl.get_dataframe())


# Categories ETL

category_etl = Category(connection)
category_etl.start_etl(df_awards)

awards_etl = Awards(connection)
awards_etl.start_etl(film_etl.get_dataframe(), df_awards_copy, category_etl.get_dataframe())

