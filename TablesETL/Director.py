import re


class Director:
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
        self.__refactor_table()
        self.dataframe = self.dataframe.drop(['id_film'], axis=1)
        self.dataframe = self.dataframe.drop_duplicates(subset=['directors'])
        self.dataframe = self.dataframe.reset_index(drop=True)
        self.dataframe['id'] = self.dataframe.index + 1

    def __load(self):
        for row in self.dataframe.to_numpy():
            query = "INSERT INTO director (id, director) " \
                    "VALUES (%s, \"%s\")" \
                    % (row[1], row[0])
            self.connection.execute(query)

    def get_dataframe(self):
        return self.dataframe

    def __refactor_table(self):
        for i, row in self.dataframe.iterrows():

            chain = row['directors']

            if "\n" in chain:
                self.dataframe.at[i, 'directors'] = chain.split("\n")
            elif self.__is_upper(chain):
                continue
            else:

                patron = r"Mc([A-Z]|$)"
                result = re.sub(patron, self.__convert_lower, chain)
                patron2 = r"([A-Z][a-z]+)\s+([A-Z][a-z]+)"
                full_names = re.findall(patron2, result)

                if not full_names:
                    full_names.append(chain)
                    self.dataframe.at[i, 'directors'] = full_names
                else:
                    # Convertir la lista de tuplas de nombres completos en una lista de strings de nombres completos
                    full_names = [" ".join(nombre) for nombre in full_names]
                    self.dataframe.at[i, 'directors'] = full_names

        self.dataframe = self.dataframe.explode('directors').reset_index(drop=True)

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
