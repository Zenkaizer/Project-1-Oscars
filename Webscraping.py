import pandas
import requests
from bs4 import BeautifulSoup


class Webscraping:

    def __init__(self):
        self.dataframe = None
        self.links = []

    def get_dataframe(self):
        self.initial_scrape()
        return self.dataframe

    def get_links(self):
        return self.links

    def initial_scrape(self):

        # URL de la página que queremos hacer webscraping
        url = "https://en.wikipedia.org/wiki/List_of_Academy_Award-winning_films"

        # Hacemos una solicitud GET a la página web
        response = requests.get(url)

        # Creamos una instancia de BeautifulSoup con el contenido HTML de la página web
        soup = BeautifulSoup(response.content, "html.parser")

        # Encontramos la tabla que contiene la lista de películas
        table = soup.find("table", {"class": "wikitable sortable"})

        # Creamos una lista vacía para almacenar los datos de las películas
        data = []

        # Iteramos sobre cada fila de la tabla (excepto la primera fila de encabezados)
        for row in table.findAll("tr")[1:]:

            # Obtenemos el nombre de la película y su enlace
            title = row.find("td").find("i").find("a").text
            link = row.find("td").find("i").find("a").get("href")

            td_list = row.findAll("td")

            # Obtenemos el año de la película (si está presente)
            year_element = td_list[1].text
            if year_element is not None:
                if "/" in year_element:
                    year = int(year_element.split("/")[0])
                else:
                    year = int(year_element)
            else:
                year = None

            # Obtenemos el número de nominaciones y premios de la película
            awards = int(td_list[2].text.split(" ")[0])
            nominations = int(td_list[3].text.split("[")[0])

            # Si llegamos al año 1980, detenemos el bucle para evitar hacer scraping de más datos
            if year == 1979:
                break

            # Añadimos los datos de la película a la lista
            data.append([title, year, awards, nominations])
            self.links.append(link)

        # Creamos un DataFrame de Pandas a partir de los datos
        self.dataframe = pandas.DataFrame(data, columns=["title", "year", "awards", "nominations"])
