import pandas


class Film:

    def __init__(self, connection):
        """
        Constructor de la clase FilmETL.
        :param connection: Conexión a la base de datos.
        """
        self.dataframe = None
        self.connection = connection

    def start_etl(self, df_initial, df_durations):
        """
        Método que inicial el proceso ETL.
        :param df_initial: Dataframe inicial con los datos de una película.
        :param df_durations: Dataframe con la duración de las películas.
        """
        self.__extract(df_initial)
        self.__transform(df_durations)
        self.__load()

    def __extract(self, df_initial):
        """
        Método de extracción, donde se extrae la información de df_initial y se almacena en la clase.
        :param df_initial: Dataframe inicial con los datos de una película.
        """
        self.dataframe = df_initial

    def __transform(self, df_durations):
        """
        Método encargado de la transformación de los datos del Dataframe.
        :param df_durations: Dataframe con la duración de las películas.
        """
        self.dataframe = pandas.merge(self.dataframe, df_durations, on='id_title')

    def __load(self):
        """
        Método encargado de la carga de los datos a la tabla "film".
        """
        for row in self.dataframe.to_numpy():
            query = "INSERT INTO film (id, title, year, awards, nominations, duration) " \
                    "VALUES (%s, \"%s\", %s, %s, %s, %s)" \
                    % (row[4], row[0], row[1], row[2], row[3], row[5])
            self.connection.execute(query)
