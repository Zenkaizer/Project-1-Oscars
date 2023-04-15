import pandas
from selenium import webdriver
from selenium.webdriver.common.by import By
import time


class Webscraping:

    def __init__(self):
        self.dataframe = pandas.DataFrame(columns=['film_title', 'film_nominations', 'film_awards', 'year'])
        self.driver = webdriver.Chrome('/chromedriver')

    def __web_scrap(self, url, year):
        self.driver.get(url)
        self.driver.maximize_window()
        time.sleep(5)

        film_title = self.driver.find_elements(By.CLASS_NAME, "film-title")
        film_nominations = self.driver.find_elements(By.CLASS_NAME, "film-nominations")
        film_awards = self.driver.find_elements(By.CLASS_NAME, "film-awards")

        for i in range(len(film_title)):
            data_col1 = film_title[i].text
            data_col2 = film_nominations[i].text
            data_col3 = film_awards[i].text

            new_row = pandas.DataFrame({'film_title': [data_col1], 'film_nominations': [data_col2],
                                        'film_awards': [data_col3], 'year': [year]})

            self.dataframe = pandas.concat([self.dataframe, new_row], ignore_index=True)
        self.driver.close()

    def scrape(self):
        for year in range(2010, 2016):
            url = f"https://www.scrapethissite.com/pages/ajax-javascript/#{year}"
            self.driver = webdriver.Chrome('/chromedriver')
            self.__web_scrap(url, year)

    def get_dataframe(self):
        self.scrape()
        return self.dataframe
