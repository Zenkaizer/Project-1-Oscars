import pandas


class FilmDirectors:

    def __init__(self, connection):
        self.dataframe = None
        self.connection = connection

    def start_etl(self, df_initial, df_directors):
        self.__extract(df_initial)

    def __extract(self, df_initial):
        self.dataframe = df_initial

    def __transform(self, df_directors):
        self.dataframe = self.dataframe.drop(['year', 'awards', 'nominations'], axis=1)
        self.dataframe['id_film'] = self.dataframe.index + 1

        self.dataframe = pandas.merge(self.dataframe, df_directors, on='id_title')

    def __load(self):
        