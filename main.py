from Connection import Connection
from ETL.FilmETL import FilmETL
from Webscraping import Webscraping
from ETL.CountriesETL import CountriesETL
from ETL.DirectorsETL import DirectorsETL
from ETL.YearsETL import YearsETL
from ETL.ProtagonistETL import ProtagonistETL

film_etl = FilmETL()
"""
with open('scripts/star_schema.sql', 'r') as file:
    for line in file.read().split(';'):
        connection.execute(line)
"""

web_scrap = Webscraping()
web_scrap.start_scrape()

df_initial = web_scrap.get_df_initial()
df_protagonists = web_scrap.get_df_protagonists()
df_directors = web_scrap.get_df_directors()
df_countries = web_scrap.get_df_countries()
df_durations = web_scrap.get_df_durations()


film_etl.start_etl(df_initial, df_durations, df_protagonists, df_directors, df_countries)

df_result = film_etl.get_dataframe()

df_result.to_csv("resultado.csv")
