import pandas
import requests
from bs4 import BeautifulSoup


class ProtagonistETL:
    def __init__(self):
        self.dataframe = None
        self.titles = []
        self.connection = None

    def extract(self, df_nomination):
        self.titles = df_nomination['title'].astype(str).tolist()

    def transform(self, links):

        # Creamos un DataFrame vacío para guardar la información
        self.dataframe = pandas.DataFrame(columns=["Title", "Protagonists"])

        aux = 0
        # Hacemos el web scraping para cada película
        for link in links:
            # Formamos la url de la página de Wikipedia
            url = f"https://en.wikipedia.org{link}"
            # Hacemos la petición a la página
            r = requests.get(url)
            # Creamos el objeto BeautifulSoup
            soup = BeautifulSoup(r.content, 'html.parser')

            # Encontramos los elementos que contienen la información de los protagonistas
            protagonist_elements = soup.find_all("table", {"class": "infobox vevent"})
            # Extraemos el nombre y apellido de los protagonistas
            protagonists = []
            for protagonist_element in protagonist_elements:
                for row in protagonist_element.find_all("tr"):
                    if row.th and "Starring" in row.th.text:
                        protagonist_links = row.td.find_all("a")
                        for protagonist_link in protagonist_links:
                            protagonist = protagonist_link.text
                            protagonists.append(protagonist)
                        break
                break
            # Agregamos la información al DataFrame
            new_row = {"Title": aux + 1, "Protagonists": protagonists}
            self.dataframe = pandas.concat([self.dataframe, pandas.DataFrame(new_row)], ignore_index=True)
            print(aux)
            aux = aux + 1

    def get_dataframe(self):
        return self.dataframe
