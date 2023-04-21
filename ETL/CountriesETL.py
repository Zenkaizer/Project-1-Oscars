import difflib
import re
import pycountry


class CountriesETL:
    def __init__(self, connection):
        self.dataframe = None
        self.connection = connection

    def start_etl(self, df_countries):
        self.__extract(df_countries)
        self.__transform()
        self.__load()

    def __extract(self, df_countries):
        self.dataframe = df_countries

    def __transform(self):
        self.dataframe = self.dataframe.drop('id_title', axis=1)
        self.dataframe = self.dataframe.T.drop_duplicates().T
        self.dataframe['id'] = self.dataframe.reset_index().index + 1

    def __load(self):
        for row in self.dataframe.to_numpy():
            query = "INSERT INTO countries (id, country) " \
                    "VALUES (%s, \"%s\")" \
                    % (row[0], row[1])
            self.connection.execute(query)

    def get_dataframe(self):
        return self.dataframe
