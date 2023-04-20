from Webscraping import Webscraping


class ETL:

    def __init__(self, connection):
        self.dataframe = None
        self.connection = connection
        self.scrap = Webscraping()

    def __extract(self):
        self.dataframe = self.scrap.get_df_initial()

    def __transform(self):
        self.dataframe['Nominations'] = self.dataframe['Nominations'].astype(int)
        self.dataframe['Awards'] = self.dataframe['Awards'].astype(int)
        self.dataframe['Year'] = self.dataframe['Year'].astype(int)

    def __load(self):
        for row in self.dataframe.to_numpy():
            query = "INSERT INTO oscars (film_title, year, film_nominations, film_awards) " \
                    "VALUES (\"%s\", %s, %s, %s)" \
                    % (row[0], row[1], row[2], row[3])
            self.connection.execute(query)

    def start(self):
        self.__extract()
        self.__transform()
        self.__load()

