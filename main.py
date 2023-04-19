from Connection import Connection
from ETL import ETL
from Webscraping import Webscraping
from TimeETL import TimeETL
webos = Webscraping()
time = TimeETL()

df = webos.get_dataframe()
print(df)
time.extract(df)
time.transform()
print(time.get_dataframe())


"""
connection = Connection()

with open('scripts/star_schema.sql', 'r') as file:
    for line in file.read().split(';'):
        connection.execute(line)

etl = ETL(connection)

etl.start()
"""
