class YearsETL:
    def __init__(self, connection):
        self.dataframe = None
        self.connection = connection

    def start_etl(self, df_nomination):
        self.__extract(df_nomination)
        self.__transform()
        self.__load()

    def __extract(self, df_nomination):
        self.dataframe = df_nomination

    def __transform(self):
        self.dataframe = self.dataframe.drop_duplicates(subset='year')
        self.dataframe = self.dataframe.reset_index(drop=True)
        self.dataframe['id'] = self.dataframe.index + 1
        self.dataframe['id'] = self.dataframe['id'].astype(int)
        self.dataframe['year'] = self.dataframe['year'].astype(int)

    def __load(self):
        for row in self.dataframe.to_numpy():
            query = "INSERT INTO years (id, year) " \
                    "VALUES (%s, %s)" \
                    % (row[1], row[0])
            self.connection.execute(query)

    def get_dataframe(self):
        return self.dataframe
