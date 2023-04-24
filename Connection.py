import mysql.connector
import configparser


class Connection:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read('resources/config.ini')
        connection_info = config['database']

        self.connection = mysql.connector.connect(
            database=connection_info['database'],
            host=connection_info['host'],
            user=connection_info['user'],
            password=connection_info['password']
        )

    def execute(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query, multi=True)
        self.connection.commit()

    def select(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result

    def close(self):
        self.connection.close()
