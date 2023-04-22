import pandas
import requests
from bs4 import BeautifulSoup


class Webscraping:

    def __init__(self):
        self.df_initial = None
        self.df_directors = None
        self.df_protagonists = None
        self.df_durations = None
        self.df_countries = None
        self.links = []

    def start_scrape(self):
        self.initial_scrape()
        self.movies_data_scrape()

    def get_df_initial(self):
        return self.df_initial

    def get_df_directors(self):
        return self.df_directors

    def get_df_protagonists(self):
        return self.df_protagonists

    def get_df_durations(self):
        return self.df_durations

    def get_df_countries(self):
        return self.df_countries

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
        self.df_initial = pandas.DataFrame(data, columns=["title", "year", "awards", "nominations"])
        self.df_initial['id_film'] = self.df_initial.index + 1

    def movies_data_scrape(self):

        # Creamos los DataFrames vacios para guardar la información.
        self.df_directors = pandas.DataFrame(columns=["id_film", "directors"])
        self.df_protagonists = pandas.DataFrame(columns=["id_film", "protagonists"])
        self.df_durations = pandas.DataFrame(columns=["id_film", "duration"])
        self.df_countries = pandas.DataFrame(columns=["id_film", "county"])

        aux = 0
        # Hacemos el web scraping para cada película.
        for link in self.links:

            # Formamos la url de la página de Wikipedia.
            url = f"https://en.wikipedia.org{link}"
            # Hacemos la petición a la página.
            r = requests.get(url)
            # Creamos el objeto BeautifulSoup.
            soup = BeautifulSoup(r.content, 'html.parser')

            # Encontramos los elementos que contienen la información.
            elements = soup.find_all("table", {"class": "infobox vevent"})

            # Creamos las listas vacias para guardar la información.
            directors = []
            protagonists = []
            countries = []
            duration = ""
            empty = "Sin datos"

            # Encontramos los elementos que contienen la información.
            elements = soup.find_all("table", {"class": "infobox vevent"})

            for element in elements:
                for row in element.find_all("tr"):

                    if row.th and "Directed by" in row.th.text:
                        # Si los directores están en el mismo <td>.
                        if row.td:
                            director = row.td.text.strip()
                            directors.append(director)

                    if row.th and "Starring" in row.th.text:
                        # Si los protagonistas están en el mismo <td>.
                        if row.td:
                            protagonist = row.td.text.strip()
                            protagonists.append(protagonist)

                    if row.th and "Running time" in row.th.text:
                        duration = row.td.text.strip()

                    if row.th and ("Country" in row.th.text or "Countries" in row.th.text):
                        # Si los países están en el mismo <td>.
                        if row.td:
                            country = row.td.text.strip()
                            countries.append(country)

            self.__is_empty(directors, empty)
            self.__is_empty(protagonists, empty)
            self.__is_empty(countries, empty)
            if duration == "":
                duration = empty

            # Agregamos la información a los DataFrames
            new_row = {"id_film": aux + 1, "directors": directors}
            self.df_directors = pandas.concat([self.df_directors, pandas.DataFrame(new_row)], ignore_index=True)
            new_row = {"id_film": aux + 1, "protagonists": protagonists}
            self.df_protagonists = pandas.concat([self.df_protagonists, pandas.DataFrame(new_row)], ignore_index=True)

            self.df_durations.loc[len(self.df_durations)] = [aux + 1, duration]

            new_row = {"id_film": aux + 1, "county": countries}
            self.df_countries = pandas.concat([self.df_countries, pandas.DataFrame(new_row)],
                                              ignore_index=True)

            print(aux)
            aux = aux + 1

    @staticmethod
    def __is_empty(vector, string):
        if not vector:
            vector.append(string)
