from ETL.DirectorsETL import DirectorsETL
from Webscraping import Webscraping
from TimeETL import TimeETL
from ETL.ProtagonistETL import ProtagonistETL
webos = Webscraping()
time = TimeETL()
pro = ProtagonistETL()
dire = DirectorsETL()

webos.start_scrape()

dire.extract(webos.get_df_directors())
dire.transform()
print(dire.get_dataframe())
dire.get_dataframe().to_csv("prueba.csv")



"""
connection = Connection()

with open('scripts/star_schema.sql', 'r') as file:
    for line in file.read().split(';'):
        connection.execute(line)

etl = ETL(connection)

etl.start()
"""
