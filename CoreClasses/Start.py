from CoreClasses.Connection import Connection
from TablesETL.Awards import Awards
from TablesETL.Category import Category
from TablesETL.FilmCountries import FilmCountries
from CoreClasses.Webscraping import Webscraping
from TablesETL.Film import Film
from TablesETL.FilmDirectors import FilmDirectors
from TablesETL.Director import Director
from TablesETL.FilmProtagonists import FilmProtagonists
from TablesETL.Protagonist import Protagonist
from TablesETL.Country import Country


class Start:

    def __init__(self):
        """
        Constructor de la clase.
        """
        self.connection = None
        self.web_scrap = None

    def start_program(self):
        """
        Método que inicia el programa principal.
        :return: None
        """
        self.__connect()
        self.__init_scrape()
        self.__start()

    def __connect(self):
        """
        Método que inicia la conexión a la base de datos.
        :return: None
        """
        # Creo la conexión con la clase Connection.
        self.connection = Connection()

        # Se abre el archivo "snowflake_schema.sql" para generar la base de datos.
        with open('scripts/snowflake_schema.sql', 'r') as file:
            for line in file.read().split(';'):
                self.connection.execute(line)

    def __init_scrape(self):
        """
        Método que inicia el proceso de webscraping.
        :return: None
        """
        self.web_scrap = Webscraping()
        self.web_scrap.start_scrape()

    def __start(self):
        """
        Método que inicia el proceso ETL de cada una de las clases correspondientes a las tablas de la base de datos.
        :return:
        """

        # Extracción de los datos.
        df_initial = self.web_scrap.get_df_initial()
        df_durations = self.web_scrap.get_df_durations()

        df_protagonists = self.web_scrap.get_df_protagonists()
        df_directors = self.web_scrap.get_df_directors()
        df_countries = self.web_scrap.get_df_countries()
        df_awards = self.web_scrap.get_df_awards()

        df_protagonists_copy = df_protagonists.copy()
        df_directors_copy = df_directors.copy()
        df_countries_copy = df_countries.copy()
        df_awards_copy = df_awards.copy()

        # Films ETL
        film_etl = Film(self.connection)
        film_etl.start_etl(df_initial, df_durations)

        # Directors ETL
        film_director_etl = FilmDirectors(self.connection)
        director_etl = Director(self.connection)

        director_etl.start_etl(df_directors)
        film_director_etl.start_etl(df_directors_copy, director_etl.get_dataframe())

        # Protagonists ETL
        film_protagonist_etl = FilmProtagonists(self.connection)
        protagonist_etl = Protagonist(self.connection)

        protagonist_etl.start_etl(df_protagonists)
        film_protagonist_etl.start_etl(df_protagonists_copy, protagonist_etl.get_dataframe())

        # Countries ETL
        film_country_etl = FilmCountries(self.connection)
        country_etl = Country(self.connection)

        country_etl.start_etl(df_countries)
        film_country_etl.start_etl(df_countries_copy, country_etl.get_dataframe())

        # Categories ETL
        category_etl = Category(self.connection)
        category_etl.start_etl(df_awards)

        awards_etl = Awards(self.connection)
        awards_etl.start_etl(film_etl.get_dataframe(), df_awards_copy, category_etl.get_dataframe())


