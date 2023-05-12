import pandas
import requests
from bs4 import BeautifulSoup


class Webscraping:

    def __init__(self):
        """
        Constructor de la clase Webscraping.
        """
        self.df_initial = None
        self.df_directors = None
        self.df_protagonists = None
        self.df_durations = None
        self.df_countries = None
        self.df_awards = None
        self.links = []  # Lista que almacena los enlaces de wikipedia de cada película.
        self.movies_names = []  # Lista que almacena los nombres de las películas.

    def start_scrape(self):
        """
        Método que inicia el webscraping de las páginas de Wikipedia sobre los oscars y oscars.org.
        :return: None
        """
        self.initial_scrape()
        self.movies_data_scrape()
        self.awards_film_scrape()

    def get_df_initial(self):
        """
        Método que obtiene el dataframe inicial.
        :return: Dataframe inicial.
        """
        return self.df_initial

    def get_df_directors(self):
        """
        Método que obtiene el dataframe de directores de las películas.
        :return: Dataframe de directores.
        """
        return self.df_directors

    def get_df_protagonists(self):
        """
        Método que obtiene el dataframe de protagonistas de las películas.
        :return: Dataframe de protagonistas.
        """
        return self.df_protagonists

    def get_df_durations(self):
        """
        Método que obtiene el dataframe de duraciones de las películas.
        :return: Dataframe de duración.
        """
        return self.df_durations

    def get_df_countries(self):
        """
        Método que obtiene el dataframe de paises de las películas.
        :return: Dataframe de paises.
        """
        return self.df_countries

    def get_df_awards(self):
        """
        Método que obtiene el dataframe de cada pelicula premiada.
        :return: Dataframe de premios.
        """
        return self.df_awards

    def initial_scrape(self):
        """
        Método que inicia el webcraping a la tabla de ganadores de los oscars de Wikipedia.
        :return: None
        """

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
            self.movies_names.append(title.strip())

        # Creamos un DataFrame de Pandas a partir de los datos
        self.df_initial = pandas.DataFrame(data, columns=["title", "year", "awards", "nominations"])
        self.df_initial['id_film'] = self.df_initial.index + 1

    def movies_data_scrape(self):
        """
        Método que inicia el webscraping para obtener los datos de cada película desde su página de Wikipedia.
        :return: None
        """

        # Creamos los DataFrames vacios para guardar la información.
        self.df_directors = pandas.DataFrame(columns=["id_film", "directors"])
        self.df_protagonists = pandas.DataFrame(columns=["id_film", "protagonists"])
        self.df_durations = pandas.DataFrame(columns=["id_film", "duration"])
        self.df_countries = pandas.DataFrame(columns=["id_film", "country"])

        aux = 0
        # Hacemos el web scraping para cada película.
        for link in self.links:

            if aux == 10:
                break

            # Formamos la url de la página de Wikipedia.
            url = f"https://en.wikipedia.org{link}"
            # Hacemos la petición a la página.
            r = requests.get(url)
            # Creamos el objeto BeautifulSoup.
            soup = BeautifulSoup(r.content, 'html.parser')

            # Creamos las listas vacias para guardar la información.
            directors = []
            protagonists = []
            countries = []
            duration = ""
            # Se define la variable "empty" para usarla en casos de que no existan datos que guardar.
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

            new_row = {"id_film": aux + 1, "country": countries}
            self.df_countries = pandas.concat([self.df_countries, pandas.DataFrame(new_row)],
                                              ignore_index=True)
            # Solo funciona como Debug y contador de cuando va a terminar el programa: Son 613 datos.
            print(aux)
            aux = aux + 1

    def awards_film_scrape(self):
        """
        Método que inicia el webcraping de los ganadores de los oscars desde la página oficial de oscars.org.
        :return: None
        """

        # Arreglo para guardar los datos
        data = []

        # Se itera desde 1980 a 2023, debido a que las películas estrenadas en 2022 tuvieron su premio en 2023
        for year in range(1980, 2024):
            url = f'https://www.oscars.org/oscars/ceremonies/{year}'
            page = requests.get(url)
            soup = BeautifulSoup(page.content, 'html.parser')
            categories = soup.find_all('div', {'class': 'view-grouping'})

            # Se recorre cada categoría para encontrar los campos con los datos
            for category in categories:
                category_name = category.find('h2').text
                data1 = category.find('h4').text
                data2 = category.find_all('span')[1].text

                # Se añade a data todos los datos obtenidos en esa iteración
                data.append({'year': year, 'category': category_name, 'winner': data1, 'winner2': data2})

                # Comprobamos el nombre de la categoría para identificar de que no quedan datos.
                if category_name == "Writing (Screenplay Written Directly for the Screen)":
                    break
                if category_name == "Writing (Original Screenplay)":
                    break

        # Transformamos el arreglo en un dataframe.
        self.df_awards = pandas.DataFrame(data)

    @staticmethod
    def __is_empty(vector, string):
        """
        Método que comprueba si una lista está vacía.
        :param vector: Lista a comparar.
        :param string: String que se va a insertar a la lista en caso de que esté vacía.
        :return: None
        """
        if not vector:
            vector.append(string)
