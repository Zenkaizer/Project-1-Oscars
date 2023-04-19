class ProtagonistETL:
    def __init__(self):
        self.dataframe = None
        self.connection = None

    def extract(self, df_nomination):
        self.dataframe = df_nomination

    def transform(self, links):

        self.dataframe['id'] = self.dataframe.index + 1
        self.dataframe = self.dataframe[['title']]

    def get_dataframe(self):
        return self.dataframe
