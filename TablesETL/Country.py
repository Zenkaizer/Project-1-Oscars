import difflib
import re
import pycountry


class Country:
    def __init__(self, connection):
        self.dataframe = None
        self.connection = connection

    def start_etl(self, df_countries):
        self.__extract(df_countries)
        self.__transform()
        self.__load()

    def __extract(self, df_countries):
        self.dataframe = df_countries

    def __transform(self):
        self.__separate_countries(self.dataframe)
        self.dataframe = self.dataframe.explode('country').reset_index(drop=True)
        self.dataframe = self.dataframe.drop(['id_film'], axis=1)
        self.dataframe = self.dataframe.drop_duplicates(subset=['country'])
        self.dataframe = self.dataframe.reset_index(drop=True)
        self.dataframe['country'] = self.dataframe['country'].replace({'US': 'United States', 'UK': 'United Kingdom',
                                                                       'Korea, Republic of': 'Korea',
                                                                       'United Kingdom[b]': 'United Kingdom',
                                                                       'Czechia': 'Czech Republic'})
        self.__spanish_country_name()

        self.dataframe = self.dataframe.reset_index(drop=True)

        self.dataframe['id'] = self.dataframe.index + 1

    def __load(self):
        for row in self.dataframe.to_numpy():
            query = "INSERT INTO country (id, country, country_es) " \
                    "VALUES (%s, \"%s\", \"%s\")" \
                    % (row[2], row[0], row[1])
            self.connection.execute(query)

    def get_dataframe(self):
        return self.dataframe

    def __separate_countries(self, df_countries):

        for i, row in df_countries.iterrows():

            aux_row = row['country']

            chain = self.__eliminate_brackets(aux_row)

            if "\n" in chain:
                df_countries.at[i, 'country'] = chain.split("\n")
            elif "/" in chain:
                df_countries.at[i, 'country'] = chain.split("/")
            elif chain == "US and UK":
                array = ["United States", "United Kingdom"]
                df_countries.at[i, 'country'] = array
            elif self.__is_upper(chain):
                continue
            else:

                country_names = self.__split_string_on_uppercase(chain)
                full_names = self.__find_closest_countries_in_list(country_names)
                full_names = self.__remove_duplicates(full_names)

                if not full_names:
                    full_names.append(chain)
                    df_countries.at[i, 'country'] = full_names
                else:
                    # Convertir la lista de tuplas de nombres completos en una lista de strings de nombres completos
                    full_names = ["".join(nombre) for nombre in full_names]
                    df_countries.at[i, 'country'] = full_names

    @staticmethod
    def __is_upper(string):
        if re.search(r'^[^A-Z]*[A-Z][^A-Z]*$', string):
            return True
        else:
            return False

    @staticmethod
    def __eliminate_brackets(string):
        string = re.sub(r'\[\d+\]', '', string)
        return string

    @staticmethod
    def __find_closest_country(string, pos):
        countries = list(pycountry.countries)
        country_names = [country.name for country in countries]
        matches = difflib.get_close_matches(string, country_names)
        if len(matches) > 0:
            closest_match = matches[0]
            closest_country = pycountry.countries.get(name=closest_match)
            return closest_country.name
        else:
            return None

    def __find_closest_countries_in_list(self, lst):
        countries = []
        for i, string in enumerate(lst):
            country = self.__find_closest_country(string, i)
            if country:
                countries.append(country)
        return countries

    @staticmethod
    def __split_string_on_uppercase(string):
        substrings = []
        current_substring = ""

        for character in string:
            if character.isupper() and current_substring:
                substrings.append(current_substring)
                current_substring = ""
            current_substring += character

        if current_substring:
            substrings.append(current_substring)

        return substrings

    @staticmethod
    def __remove_duplicates(arr):
        unique_items = set(arr)
        return list(unique_items)

    def __spanish_country_name(self):
        countries = {
            'Argentina': 'Argentina',
            'Australia': 'Australia',
            'Austria': 'Austria',
            'Belgium': 'Bélgica',
            'Bosnia and Herzegovina': 'Bosnia y Herzegovina',
            'Brazil': 'Brasil',
            'Canada': 'Canadá',
            'Chile': 'Chile',
            'China': 'China',
            'Czech Republic': 'República Checa',
            'Denmark': 'Dinamarca',
            'England': 'Inglaterra',
            'Finland': 'Finlandia',
            'France': 'Francia',
            'Germany': 'Alemania',
            'Hong Kong': 'Hong Kong',
            'Hungary': 'Hungría',
            'India': 'India',
            'Iran': 'Irán',
            'Ireland': 'Irlanda',
            'Italy': 'Italia',
            'Japan': 'Japón',
            'Kenya': 'Kenia',
            'Korea': 'Corea del Sur',
            'Luxembourg': 'Luxemburgo',
            'Mexico': 'México',
            'Morocco': 'Marruecos',
            'Netherlands': 'Países Bajos',
            'New Zealand': 'Nueva Zelanda',
            'Norway': 'Noruega',
            'Pakistan': 'Pakistán',
            'Peru': 'Perú',
            'Poland': 'Polonia',
            'Portugal': 'Portugal',
            'Réunion': 'Unión Soviética',
            'Romania': 'Rumania',
            'Sin datos': 'Sin datos',
            'Slovenia': 'Eslovenia',
            'South Africa': 'Sudáfrica',
            'South Sudan': 'Sudán del Sur',
            'Spain': 'España',
            'Sweden': 'Suecia',
            'Switzerland': 'Suiza',
            'Taiwan': 'Taiwán',
            'Tunisia': 'Túnez',
            'Turkey': 'Turquía',
            'United Arab Emirates': 'Emiratos Árabes Unidos',
            'United Kingdom': 'Reino Unido',
            'United States': 'Estados Unidos',
            'West Germany': 'Alemania Occidental',
            'Zimbabwe': 'Zimbabue'
        }
        self.dataframe['country_es'] = self.dataframe['country'].map(countries)
