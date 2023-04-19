class TimeETL:
    def __init__(self):
        self.dataframe = None
        self.connection = None

    def extract(self, df_nomination):
        self.dataframe = df_nomination

    def transform(self):
        self.dataframe = self.dataframe.drop_duplicates(subset='year')
        self.dataframe = self.dataframe.reset_index(drop=True)
        self.dataframe['id'] = self.dataframe.index + 1
        self.dataframe = self.dataframe[['year', 'id']]

    def get_dataframe(self):
        return self.dataframe
