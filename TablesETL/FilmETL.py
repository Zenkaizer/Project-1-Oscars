import difflib
import re

import pandas
import pycountry


class FilmETL:

    def __init__(self):
        self.dataframe = None
        self.connection = None

    def start_etl(self, df_initial, df_duration, df_protagonists, df_directors, df_countries):
        self.__extract(df_initial)
        self.__transform(df_duration, df_protagonists, df_directors, df_countries)

    def get_dataframe(self):
        return self.dataframe

    def __extract(self, df_initial):
        self.dataframe = df_initial

    def __transform(self, df_duration, df_protagonists, df_directors, df_countries):

        df_duration = self.__transform_duration(df_duration)
        df_protagonists = self.__transform_protagonist(df_protagonists)
        df_directors = self.__transform_directors(df_directors)
        df_countries = self.__transform_countries(df_countries)

        self.dataframe['id_title'] = self.dataframe.reset_index().index + 1
        self.dataframe = pandas.merge(self.dataframe, df_duration, on='id_title')
        self.dataframe = pandas.merge(self.dataframe, df_protagonists, on='id_title')
        self.dataframe = pandas.merge(self.dataframe, df_directors, on='id_title')
        self.dataframe = pandas.merge(self.dataframe, df_countries, on='id_title')

    def __transform_duration(self, df_duration):
        for i, row in df_duration.iterrows():
            chain = row['duration']
            chain = self.__get_numbers(chain)
            df_duration.at[i, 'duration'] = chain

        df_duration['id_title'] = df_duration['id_title'].astype(int)
        df_duration['duration'] = df_duration['duration'].astype(int)
        return df_duration

    def __transform_protagonist(self, df_protagonists):

        for i, row in df_protagonists.iterrows():

            chain = row['protagonists']

            if "\n" in chain:
                df_protagonists.at[i, 'protagonists'] = chain.split("\n")
            elif self.__is_upper(chain):
                continue
            elif chain == "Derek de LintMarc van UchelenMonique van de Ven":
                array = ["Derek de Lint", "Marc van Uchelen", "Monique van de Ven"]
                df_protagonists.at[i, 'protagonists'] = array
            else:

                patron = r"Mc([A-Z]|$)"
                result = re.sub(patron, self.__convert_lower, chain)
                patron2 = r"([A-Z][a-z]+)\s+([A-Z][a-z]+)"
                full_names = re.findall(patron2, result)

                if not full_names:
                    full_names.append(chain)
                    df_protagonists.at[i, 'protagonists'] = full_names
                else:
                    # Convertir la lista de tuplas de nombres completos en una lista de strings de nombres completos
                    full_names = [" ".join(nombre) for nombre in full_names]
                    df_protagonists.at[i, 'protagonists'] = full_names

        df_protagonists = df_protagonists.explode('protagonists').reset_index(drop=True)

        df_protagonists['id_title'] = df_protagonists['id_title'].astype(int)
        return df_protagonists

    def __transform_countries(self, df_countries):
        self.__separate_countries(df_countries)
        df_countries = df_countries.explode('county').reset_index(drop=True)
        df_countries['id_title'] = df_countries['id_title'].astype(int)
        return df_countries

    @staticmethod
    def __get_numbers(chain):
        # Expresión regular que busca números al comienzo de la cadena
        match = re.match(r'\d+', chain)

        if match:
            # Si se encuentra una coincidencia, se devuelve solo los números encontrados
            return int(match.group(0))
        else:
            # Si no se encuentra una coincidencia, se devuelve -1
            return -1

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

    def __separate_countries(self, df_countries):

        for i, row in df_countries.iterrows():

            aux_row = row['county']

            chain = self.__eliminate_brackets(aux_row)

            if "\n" in chain:
                df_countries.at[i, 'county'] = chain.split("\n")
            elif "/" in chain:
                df_countries.at[i, 'county'] = chain.split("/")
            elif self.__is_upper(chain):
                continue
            else:

                country_names = self.__split_string_on_uppercase(chain)
                full_names = self.__find_closest_countries_in_list(country_names)
                full_names = self.__remove_duplicates(full_names)

                if not full_names:
                    full_names.append(chain)
                    df_countries.at[i, 'county'] = full_names
                else:
                    # Convertir la lista de tuplas de nombres completos en una lista de strings de nombres completos
                    full_names = ["".join(nombre) for nombre in full_names]
                    df_countries.at[i, 'county'] = full_names[0]

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
