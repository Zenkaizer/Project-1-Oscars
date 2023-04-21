import re


class DirectorsETL:
    def __init__(self, connection):
        self.dataframe = None
        self.connection = connection

    def start_etl(self, df_directors):
        self.__extract(df_directors)
        self.__transform()
        self.__load()

    def __extract(self, df_directors):
        self.dataframe = df_directors

    def __transform(self):
        self.dataframe = self.dataframe.drop('id_title', axis=1)
        self.dataframe = self.dataframe.T.drop_duplicates().T
        self.dataframe['id'] = self.dataframe.reset_index().index + 1

    def __load(self):
        for row in self.dataframe.to_numpy():
            query = "INSERT INTO directors (id_title, protagonists) " \
                    "VALUES (%s, \"%s\")" \
                    % (row[0], row[1])
            self.connection.execute(query)

    def get_dataframe(self):
        return self.dataframe
