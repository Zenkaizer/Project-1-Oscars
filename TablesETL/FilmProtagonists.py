import re


class FilmProtagonists:

    def __init__(self, connection):
        self.dataframe = None
        self.connection = connection

    def start_etl(self, df_protagonists, new_df_protagonists):
        self.__extract(df_protagonists)
        self.__transform(new_df_protagonists)
        self.__load()

    def __extract(self, df_protagonists):
        self.dataframe = df_protagonists

    def __transform(self, new_df_protagonists):

        self.__refactor_table()
        mapping = dict(zip(new_df_protagonists['protagonists'], new_df_protagonists['id']))
        self.dataframe['id_protagonist'] = self.dataframe['protagonists'].map(mapping)
        self.dataframe = self.dataframe.drop('protagonists', axis=1)

    def __load(self):
        for row in self.dataframe.to_numpy():
            query = "INSERT INTO film_protagonist (id_film, id_protagonist) " \
                    "VALUES (%s, %s)" \
                    % (row[0], row[1])
            self.connection.execute(query)

    def __refactor_table(self):
        for i, row in self.dataframe.iterrows():

            chain = row['protagonists']
            chain = chain.replace('"', '')

            if "\n" in chain:
                self.dataframe.at[i, 'protagonists'] = chain.split("\n")
            elif self.__is_upper(chain):
                continue
            elif chain == "Derek de LintMarc van UchelenMonique van de Ven":
                array = ["Derek de Lint", "Marc van Uchelen", "Monique van de Ven"]
                self.dataframe.at[i, 'protagonists'] = array
            else:

                patron = r"Mc([A-Z]|$)"
                result = re.sub(patron, self.__convert_lower, chain)
                patron2 = r"([A-Z][a-z]+)\s+([A-Z][a-z]+)"
                full_names = re.findall(patron2, result)

                if not full_names:
                    full_names.append(chain)
                    self.dataframe.at[i, 'protagonists'] = full_names
                else:
                    # Convertir la lista de tuplas de nombres completos en una lista de strings de nombres completos
                    full_names = [" ".join(nombre) for nombre in full_names]
                    self.dataframe.at[i, 'protagonists'] = full_names

        self.dataframe = self.dataframe.explode('protagonists').reset_index(drop=True)

    @staticmethod
    def __convert_lower(match):
        if match.group(1) == "":
            return "Mc"
        else:
            return "Mc" + match.group(1).lower()

    @staticmethod
    def __is_upper(string):
        if re.search(r'^[^A-Z]*[A-Z][^A-Z]*$', string):
            return True
        else:
            return False
