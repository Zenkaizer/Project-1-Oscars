import numpy as np


class Awards:

    def __init__(self, connection):
        """
        Constructor de la clase Awards.
        :param connection: Instancia de la conexión a la base de datos.
        """
        self.dataframe = None
        self.connection = connection

    def start_etl(self, df_initial, df_awards, df_categories):
        """
        Método que inicia el proceso ETL para la tabla awards.
        :param df_initial: Dataframe inicial.
        :param df_awards: Dataframe de premios.
        :param df_categories: Dataframe de categorias de cada premio.
        :return: None
        """
        self.__extract(df_awards)
        self.__transform(df_initial, df_categories)
        self.__load()

    def __extract(self, df_awards):
        """
        Método encargado de la extracción de los datos.
        :param df_awards: Dataframe de premios.
        :return: None
        """
        self.dataframe = df_awards

    def __transform(self, df_initial, df_categories):
        """
        Método que realiza la transformación de los datos.
        :param df_initial: Dataframe inicial.
        :param df_categories: Dataframe de categorias de premios.
        :return: None
        """
        # Se hace un reemplazo de los string del dataframe que contengan un salto de linea por vacío
        self.dataframe = self.dataframe.applymap(lambda x: x.replace('\n', '') if isinstance(x, str) else x)
        self.__normalize_titles()

        # Se eliminan los espacios laterales de la columna de títulos
        df_initial['title'] = df_initial['title'].str.strip()

        # Como no se sabe si en la columna "winner" o "winner2" está el título de la película
        # Se verifica que esté en una de las dos
        self.dataframe['id_film'] = self.dataframe.apply(lambda row: df_initial['id_film'][df_initial['title'].
                                                         isin([row['winner'], row['winner2']])].
                                                         values[0] if any(df_initial['title'].
                                                                          isin([row['winner'],
                                                                                row['winner2']])) else None, axis=1)

        # Se hace un mapeo entre el dataframe de categorias y el dataframe de premios
        # para reemplazar el nombre de la categoría por el id.
        mapping = dict(zip(df_categories['category'], df_categories['id']))
        self.dataframe['id_category'] = self.dataframe['category'].map(mapping)

        # Se dropean los datos y columnas que no importan
        self.dataframe = self.dataframe.drop(['category', 'winner', 'winner2'], axis=1)
        self.dataframe.replace(" ", np.nan, inplace=True)
        self.dataframe.dropna(inplace=True)
        self.dataframe = self.dataframe.reset_index(drop=True)

        # Se crea una columna "id" para el dataframe
        self.dataframe['id'] = self.dataframe.index + 1

    def __load(self):
        """
        Método que realiza la carga a la base de datos.
        :return: None
        """
        for row in self.dataframe.to_numpy():
            query = "INSERT INTO awards (id, year, id_film, id_category) " \
                    "VALUES (%s, %s, %s, %s)" \
                    % (row[3], row[0], row[1], row[2])
            self.connection.execute(query)

    def get_dataframe(self):
        """
        Método que obtiene el dataframe de la clase.
        :return: Dataframe de los premios.
        """
        return self.dataframe

    def __normalize_titles(self):
        """
        Método que normaliza los títulos que desde la pagina de "oscars.org" no coinciden con los extraidos
        desde Wikipedia.
        :return: None
        """
        names = {
            "Volver a Empezar ('To Begin Again')": "To Begin Again (Volver a empezar)",
            "E.T. The Extra-Terrestrial": "E.T.: The Extra-Terrestrial",
            "Women--for America, for the World": "Women – for America, for the World",
            "Herbie Hancock": "Round Midnight",
            "The Ten-Year Lunch: The Wit and Legend of the Algonquin Round Table": "The Ten-Year Lunch",
            "Hotel Terminus: The Life and Times of Klaus Barbie": "Hôtel Terminus: The Life and Times of Klaus Barbie",
            "Good Fellas": "Goodfellas",
            "Dances With Wolves": "Dances with Wolves",
            "A River Runs through It": "A River Runs Through It",
            "Pepe Danquart": "Black Rider (Schwarzfahrer)",
            "Bullets over Broadway": "Bullets Over Broadway",
            "The Postman (Il Postino)": "Il Postino: The Postman",
            "The Personals: Improvisations on Romance in the Golden Years": "The Personals",
            "Kim Magnusson, Anders Thomas Jensen": "Election Night (Valgaften)",
            "All about My Mother": "All About My Mother (Todo sobre mi madre)",
            "Dr. Seuss' How the Grinch Stole Christmas": "How the Grinch Stole Christmas",
            "Quiero Ser (I want to be...)": "Quiero ser (I want to be...)",
            "Moulin Rouge": "Moulin Rouge!",
            "the accountant": "The Accountant",
            "This Charming Man (Der Er En Yndig Mand)": "This Charming Man (Der Er En Yndig Mand)",
            "Wallace & Gromit in The Curse of the Were-Rabbit": "Wallace & Gromit: The Curse of the Were-Rabbit",
            "La Vie en Rose": "La Vie en rose",
            "Sweeney Todd The Demon Barber of Fleet Street": "Sweeney Todd: The Demon Barber of Fleet Street",
            "Le Mozart des Pickpockets (The Mozart of Pickpockets)": "Le Mozart des pickpockets",
            "Spielzeugland (Toyland)": "Toyland",
            "Precious: Based on the Novel 'Push' by Sapphire": "Precious",
            "Les Mis_rables": "Les Misérables",
            "The Lady in Number 6: Music Saved My Life": "The Lady in Number 6",
            "Mr. Hublot": "Mr Hublot",
            "CitizenFour": "Citizenfour",
            "from La La Land; Music by Justin Hurwitz; Lyric by Benj Pasek and Justin Paul": "La La Land",
            "Three Billboards outside Ebbing, Missouri": "Three Billboards Outside Ebbing, Missouri",
            "from Coco; Music and Lyric by Kristen Anderson-Lopez and Robert Lopez": "Coco",
            "from A Star Is Born; Music and Lyric by Lady Gaga, Mark Ronson, Anthony Rossomando and Andrew Wyatt": "A Star Is Born",
            "Once upon a Time...in Hollywood": "Once Upon a Time in Hollywood",
            "from Rocketman; Music by Elton John; Lyric by Bernie Taupin": "Rocketman",
            "from Judas and the Black Messiah; Music by H.E.R. and Dernst Emile II; Lyric by H.E.R. and Tiara Thomas": "Judas and the Black Messiah",
            "Summer of Soul (...Or, When the Revolution Could Not Be Televised)": "Summer of Soul",
            "No Time To Die": "No Time to Die",
            "from RRR; Music by M.M. Keeravaani; Lyric by Chandrabose": "RRR"
        }
        self.dataframe['winner'] = self.dataframe['winner'].replace(names)
        self.dataframe['winner2'] = self.dataframe['winner2'].replace(names)
