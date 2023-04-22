import re

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
        df_durations = self.__transform_duration(df_durations)
        self.dataframe = pandas.merge(self.dataframe, df_durations, on='id_film')

    def __load(self):
        """
        Método encargado de la carga de los datos a la tabla "film".
        """
        for row in self.dataframe.to_numpy():
            query = "INSERT INTO film (id, title, year, awards, nominations, duration) " \
                    "VALUES (%s, \"%s\", %s, %s, %s, %s)" \
                    % (row[4], row[0], row[1], row[2], row[3], row[5])
            self.connection.execute(query)

    def __transform_duration(self, df_duration):
        for i, row in df_duration.iterrows():
            chain = row['duration']
            chain = self.__get_numbers(chain)
            df_duration.at[i, 'duration'] = chain

        df_duration['id_film'] = df_duration['id_film'].astype(int)
        df_duration['duration'] = df_duration['duration'].astype(int)
        return df_duration

    @staticmethod
    def __get_numbers(chain):
        # Expresión regular que busca números al comienzo de la cadena
        match = re.match(r'\d+', chain)

        if match:
            # Si se encuentra una coincidencia, se devuelve solo los números encontrados
            return int(match.group(0))
        else:
            # Si no se encuentra una coincidencia, se devuelve -1
            return -1
