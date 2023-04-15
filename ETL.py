import pandas
from Webscraping import Webscraping


class ETL:

    def __init__(self, connection):
        self.dataframe = None
        self.connection = connection
        self.webscrap = Webscraping()

    def __extract(self):
        self.dataframe = self.webscrap.get_dataframe()

    def __transform(self):
        self.dataframe['film_nominations'] = self.dataframe['film_nominations'].astype(int)
        self.dataframe['film_awards'] = self.dataframe['film_awards'].astype(int)
        self.dataframe['year'] = self.dataframe['year'].astype(int)

    def __load(self):
        for row in self.dataframe.to_numpy():
            query = "INSERT INTO oscars (film_title, film_nominations, film_awards, year) " \
                    "VALUES (\"%s\", %s, %s, %s)" \
                    % (row[0], row[1], row[2], row[3])
            self.connection.execute(query)

    def start(self):
        self.__extract()
        self.__transform()
        self.__load()
