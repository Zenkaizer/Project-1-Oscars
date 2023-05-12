import mysql.connector
import configparser


class Connection:

    def __init__(self):
        """
        Constructor de la clase Connection.
        """
        # Se lee el archivo "config.ini" para obtener las credenciales de la base de datos.
        config = configparser.ConfigParser()
        config.read('resources/config.ini')
        connection_info = config['database']

        # Se crea la conexión con la libreria mysql.connector.
        self.connection = mysql.connector.connect(
            database=connection_info['database'],
            host=connection_info['host'],
            user=connection_info['user'],
            password=connection_info['password']
        )

    def execute(self, query):
        """
        Método que sirve para ejecutar sentencias SQL.
        :param query: Sentencia SQL a ejecutar en la base de datos.
        :return: None
        """
        cursor = self.connection.cursor()
        cursor.execute(query, multi=True)
        self.connection.commit()

    def close(self):
        """
        Método que cierra/apaga la conexión a la base de datos.
        :return: None
        """
        self.connection.close()
