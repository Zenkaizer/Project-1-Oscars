
class Category:

    def __init__(self, connection):
        self.dataframe = None
        self.connection = connection

    def start_etl(self, df_awards):
        self.__extract(df_awards)
        self.__transform()
        self.__load()

    def __extract(self, df_awards):
        self.dataframe = df_awards

    def __transform(self):
        self.dataframe = self.dataframe.drop(['year', 'winner', 'winner2'], axis=1)
        self.dataframe = self.dataframe.drop_duplicates(subset=['category'])
        self.dataframe = self.dataframe.reset_index(drop=True)
        categories = {
            'Actor in a Leading Role': 'Actor en un Papel Principal',
            'Actor in a Supporting Role': 'Actor en un Papel de Apoyo',
            'Actress in a Leading Role': 'Actriz en un Papel Principal',
            'Actress in a Supporting Role': 'Actriz en un Papel de Apoyo',
            'Art Direction': 'Dirección de Arte',
            'Cinematography': 'Cinematografía',
            'Costume Design': 'Diseño de Vestuario',
            'Directing': 'Dirección',
            'Documentary (Feature)': 'Documental (Largometraje)',
            'Documentary (Short Subject)': 'Documental (Cortometraje)',
            'Film Editing': 'Edición de Cine',
            'Foreign Language Film': 'Película en Idioma Extranjero',
            'Irving G. Thalberg Memorial Award': 'Premio Irving G. Thalberg Memorial',
            'Jean Hersholt Humanitarian Award': 'Premio Humanitario Jean Hersholt',
            'Music (Original Score)': 'Música (Banda Sonora Original)',
            'Music (Original Song Score and Its Adaptation -or- Adaptation Score)': 'Música (Puntuación de Canción Original y su Adaptación -o- Puntuación de Adaptación)',
            'Music (Original Song)': 'Música (Canción Original)',
            'Best Picture': 'Mejor Película',
            'Short Film (Animated)': 'Cortometraje (Animado)',
            'Short Film (Live Action)': 'Cortometraje (Acción en Vivo)',
            'Sound': 'Sonido',
            'Special Achievement Award (Sound Editing)': 'Premio de Logro Especial (Edición de Sonido)',
            'Visual Effects': 'Efectos Visuales',
            'Writing (Screenplay Based on Material from Another Medium)': 'Escritura (Guión basado en Material de Otro Medio)',
            'Writing (Screenplay Written Directly for the Screen)': 'Escritura (Guión Escrito Directamente para la Pantalla)',
            'Short Film (Dramatic Live Action)': 'Cortometraje (Dramático Acción en Vivo)',
            'Special Achievement Award (Visual Effects)': 'Premio de Logro Especial (Efectos Visuales)',
            'Makeup': 'Maquillaje',
            'Special Achievement Award (Sound Effects Editing)': 'Premio de Logro Especial (Edición de Efectos de Sonido)',
            'Sound Effects Editing': 'Edición de Efectos de Sonido',
            'Music (Original Song Score or Adaptation Score)': 'Música (Puntuación de Canción Original o Puntuación de Adaptación)',
            'Music (Original Song Score)': 'Música (Puntuación de Canción Original)',
            'Special Achievement Award': 'Premio de Logro Especial',
            'Writing (Screenplay Based on Material Previously Produced or Published)': 'Escritura (Guión basado en Material Previamente Producido o Publicado)',
            'Music (Original Dramatic Score)': 'Música (Puntuación Dramática Original)',
            'Music (Original Musical or Comedy Score)': 'Música (Puntuación Original de Musical o Comedia)',
            'Sound Editing': 'Edición de Sonido',
            'Animated Feature Film': 'Película de Animación',
            'Writing (Adapted Screenplay)': 'Escritura (Guión Adaptado)',
            'Writing (Original Screenplay)': 'Escritura (Guión Original)',
            'Sound Mixing': 'Mezcla de Sonido',
            'Makeup and Hairstyling': 'Maquillaje y Peinado',
            'Production Design': 'Diseño de Producción',
            'International Feature Film': 'Película Internacional',
            'Documentary Feature Film': 'Documental (Largometraje)',
            'Documentary Short Film': 'Documental (Cortometraje)',
            'Papito': 'Término cariñoso en español para padre o papá'
        }
        self.dataframe['category_es'] = self.dataframe['category'].map(categories)
        self.dataframe['id'] = self.dataframe.index + 1

    def __load(self):
        for row in self.dataframe.to_numpy():
            query = "INSERT INTO category (id, category, category_es) " \
                    "VALUES (%s, \"%s\", \"%s\")" \
                    % (row[2], row[0], row[1])
            self.connection.execute(query)

    def get_dataframe(self):
        return self.dataframe
