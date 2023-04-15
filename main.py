from Connection import Connection
from ETL import ETL

connection = Connection()

with open('scripts/star_schema.sql', 'r') as file:
    for line in file.read().split(';'):
        connection.execute(line)

etl = ETL(connection)

etl.start()
