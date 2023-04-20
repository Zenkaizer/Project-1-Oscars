from Connection import Connection
from ETL import ETL
from Webscraping import Webscraping
from TimeETL import TimeETL
import pandas
from ProtagonistETL import ProtagonistETL
webos = Webscraping()
time = TimeETL()
pro = ProtagonistETL()

webos.start_scrape()

pro.extract(webos.get_df_protagonists())
pro.transform()
print(pro.get_dataframe())
pro.get_dataframe().to_csv("prueba.csv")



"""
connection = Connection()

with open('scripts/star_schema.sql', 'r') as file:
    for line in file.read().split(';'):
        connection.execute(line)

etl = ETL(connection)

etl.start()
"""
