import time
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromiumService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType

class FotmobScraper:
    def __init__(self, season_url):
        self.season_url = season_url

    def get_match_ids_for_round(self, round):
        match_ids = []

        # run Chrome in headless mode
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")

        links = None
        # create chrome driver context manager for scraping
        with webdriver.Chrome(service=ChromiumService(ChromeDriverManager(driver_version="125.0.6422.78", chrome_type=ChromeType.CHROMIUM).install()), options=options) as browser:
            # open page for matches for the current round
            browser.get(f'{self.season_url}&round={round}')
            time.sleep(5)

            # extract the match ids of each match in the round
            links = browser.find_elements(By.CLASS_NAME, 'css-hvo6tv-MatchWrapper')
        
        for link in links:
            match_id = link.get_attribute('href').split('#')[1]
            if (match_id not in match_ids):
                match_ids.append(match_id)
        
        return match_ids

    def get_available_rounds(self):
        # run Chrome in headless mode
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")

        available_rounds = None
        # create chrome driver context manager for scraping
        with webdriver.Chrome(service=ChromiumService(ChromeDriverManager(driver_version="125.0.6422.78", chrome_type=ChromeType.CHROMIUM).install()), options=options) as browser:
            # open premier league latest season matches on Fotmob
            browser.get(self.season_url)

            # extract available rounds
            select_element = browser.find_element(By.XPATH, "html/body/div/main/main/section/div/div[2]/section/div/div[1]/div/select")
            select = Select(select_element)
            options = select.options
            available_rounds = [option.get_attribute('value') for option in options]
        
        return available_rounds

    def get_match_data(self, match_id):
        params = {
            "matchId": match_id
        }
        response = requests.get('https://www.fotmob.com/api/matchDetails', params=params)
        return response.json()
