
class Category:

    def __init__(self, connection):
        self.dataframe = None
        self.connection = connection

    def start_etl(self, df_categories):
        self.__extract(df_categories)
        self.__transform()
        self.__load()

    def __extract(self, df_categories):
        self.dataframe = df_categories

    def __transform(self):
        categories = {
            'Best Picture': 'Mejor película',
            'Best Director': 'Mejor director',
            'Best Actor': 'Mejor actor',
            'Best Actress': 'Mejor actriz',
            'Best Cinematography': 'Mejor fotografía',
            'Best Production Design': 'Mejor diseño de producción',
            'Best Adapted Screenplay': 'Mejor guion adaptado',
            'Best Sound': 'Mejor sonido',
            'Best Animated Short Film': 'Mejor cortometraje animado',
            'Best Live Action Short Film': 'Mejor cortometraje de acción real',
            'Best Film Editing': 'Mejor edición',
            'Best Original Score': 'Mejor música original',
            'Best Original Song': 'Mejor canción original',
            'Best Supporting Actor': 'Mejor actor de reparto',
            'Best Supporting Actress': 'Mejor actriz de reparto',
            'Best Visual Effects': 'Mejores efectos visuales',
            'Best Original Screenplay': 'Mejor guion original',
            'Best Documentary Short Film': 'Mejor cortometraje documental',
            'Best Documentary Feature Film': 'Mejor largometraje documental',
            'Best International Feature Film': 'Mejor película internacional',
            'Best Costume Design': 'Mejor diseño de vestuario',
            'Best Makeup and Hairstyling': 'Mejor maquillaje y peluquería',
            'Best Animated Feature Film': 'Mejor largometraje animado'
        }
        self.dataframe['category_es'] = self.dataframe['category'].map(categories)
        self.dataframe['id'] = self.dataframe.index + 1

    def __load(self):
        for row in self.dataframe.to_numpy():
            query = "INSERT INTO category (id, category, category_es) " \
                    "VALUES (%s, \"%s\", \"%s\")" \
                    % (row[2], row[0], row[1])
            self.connection.execute(query)

