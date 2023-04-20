from Webscraping import Webscraping
import re


class ProtagonistETL:
    def __init__(self):
        self.dataframe = None
        self.connection = None

    def extract(self, df_protagonists):
        self.dataframe = df_protagonists

    def transform(self):

        for i, row in self.dataframe.iterrows():
            if "\n" in row['protagonists']:
                self.dataframe.at[i, 'protagonists'] = row['protagonists'].split("\n")

            elif "Sin datos" in row['protagonists']:
                continue
            else:

                chain = row['protagonists']
                patron = r"Mc([A-Z]|$)"
                result = re.sub(patron, self.__convertir_minuscula, chain)
                patron2 = r"([A-Z][a-z]+)\s+([A-Z][a-z]+)"
                nombres_completos = re.findall(patron2, result)

                # Convertir la lista de tuplas de nombres completos en una lista de strings de nombres completos
                nombres_completos = [" ".join(nombre) for nombre in nombres_completos]
                self.dataframe.at[i, 'protagonists'] = nombres_completos

        self.dataframe = self.dataframe.explode('protagonists').reset_index(drop=True)

    def get_dataframe(self):
        return self.dataframe

    @staticmethod
    def __convertir_minuscula(match):
        if match.group(1) == "":
            return "Mc"
        else:
            return "Mc" + match.group(1).lower()
