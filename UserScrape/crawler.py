
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from datetime import datetime
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from pathlib import Path
from time import sleep

class Crawler:
    def __init__(self, driver:WebDriver, sas_url:str):
        self._video_infos = {}
        self.wait = WebDriverWait(driver, 10)
        self.driver = driver
        self.sessionPath = ''
        self.container = ContainerClient.from_container_url(sas_url)

    # easy method to save screenshots for headless mode
    def log_info(self, name:str):
        localDir = f'/tmp/crawler/{self.sessionPath}'

        

        Path(localDir).mkdir(parents=True, exist_ok=True)

        wd = self.driver

        imageFile = f'{localDir}/{name}.png'
        with open(imageFile, 'w') as f:
            wd.get_screenshot_as_file(imageFile)
        #with open(imageFile, 'r') as f:
        #    self.container.upload_blob(f'{self.sessionPath}/{name}.png', f)

        htmlFile = f'{localDir}/{name}.html'
        with open(htmlFile, "w") as f:
            f.write(wd.page_source)
        #with open(imageFile, 'r') as f:
        #    self.container.upload_blob(f'{self.sessionPath}/{name}.html', f)

        print(f'{name} - {self.sessionPath} ',)

    def login(self, email:str, password, lang = 'en'):
        wd = self.driver
        user = email.split("@")[0]
        self.sessionPath = f'{datetime.now().strftime("%Y%m%d-%H%M%S")}.{user}.{wd.session_id}'

         # this link is maybe too specific (e.g. it contains country codes)
        wd.get(f'https://accounts.google.com/signin/v2/identifier?service=youtube&uilel=3&passive=true&continue=https%3A%2F%2Fwww.youtube.com%2Fsignin%3Faction_handle_signin%3Dtrue%26app%3Ddesktop%26hl%3D{lang}%26next%3D%252F&hl={lang}&ec=65620&flowName=GlifWebSignIn&flowEntry=ServiceLogin')
        self.log_info(f'landing')

        wd.find_element_by_css_selector('input[type="email"]').send_keys(email) #login_email = wd.find_element_by_id('Email').send_keys(email)
        wd.find_element_by_css_selector('#identifierNext').click() #next_button = wd.find_element_by_id('next').click()

        sleep(3) # sleep to recorded html is post click events
        self.log_info('email_entered')

        password = WebDriverWait(wd, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[type="password"]'))
        ).send_keys(password)
        wd.find_element_by_css_selector('#passwordNext').click()
        sleep(3)
        self.log_info('password_entered')

        # non-headless
        # login_email = driver.find_element_by_id('identifierId').send_keys(user)
        # next_button = driver.find_element_by_id('identifierNext').click()
        # password = WebDriverWait(driver, 5).until(
        #     EC.element_to_be_clickable((By.XPATH, "//input[@name='password']"))
        # ).send_keys(password)
        # password_next_button = driver.find_element_by_id('passwordNext').click()

    def get_n_search_results(self, search_term, max_results=5, order="relevance"):
        wd = self.driver

        search_bar = WebDriverWait(wd, 10).until(
            EC.element_to_be_clickable((By.ID, "search"))
        ).send_keys(search_term)
        search_button = wd.find_element_by_id('search-icon-legacy').click()

        # Wait until the search results are loaded
        results_content = WebDriverWait(wd, 10).until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="filter-menu"]')))
        all_videos = WebDriverWait(wd, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, '//*[@id="video-title"]'))
        )
        links = []
        for i in all_videos:
            # somehow there are also empty elements with the id video-title found, these need to be removed
            if len(i.text) != 0:
                full_link = i.get_attribute('href')
                links.append(full_link.replace('https://www.youtube.com/watch?v=', ''))
        return links[0:max_results]

    def get_n_recommendations(self, seed, depth, branching=5):
        if depth == 0:
            self.driver.get("https://www.youtube.com/watch?v=" + seed)
            self.get_video_features(seed, [])
            return [seed]
        current_video = seed
        all_recos = [seed]
        for video in self.get_recommendations_for_video(source=current_video, branching=branching):
            all_recos.extend(self.get_n_recommendations(video, depth - 1))
        return all_recos

    def get_video_features(self, id, recommendations):
        if id in self._video_infos:
            self._video_infos[id]['recommendations'] = self._video_infos[id]['recommendations'] + \
                                                       list(set(recommendations) - set(
                                                           self._video_infos[id]['recommendations']))
        if id not in self._video_infos:
            self._video_infos[id] = {'recommendations': recommendations,
                                     'title': self.wait.until(EC.presence_of_element_located(
                                         (By.CSS_SELECTOR, "#container > h1 > yt-formatted-string"))).text,
                                     'id': id,
                                     'channel': self.wait.until(EC.presence_of_element_located(
                                         (By.CSS_SELECTOR,
                                          "ytd-channel-name.ytd-video-owner-renderer > div:nth-child(1) > "
                                          "div:nth-child(1)"))).text,
                                     'channel_id': self.wait.until(EC.presence_of_element_located(
                                         (By.CSS_SELECTOR, "#text > a"))).get_attribute('href').strip(
                                         'https://www.youtube.com/channel/')
                                     }

    def get_recommendations_for_video(self, source, branching):
        self.driver.get("https://www.youtube.com/watch?v=" + source)
        results_content = WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, '// *[ @ id = "upnext"]')))
        all_recs = WebDriverWait(self.driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, '//*[@id="dismissable"]/div/div[1]/a'))
        )

        recos = []
        for i in all_recs:
            # somehow there are also empty elements with the id video-title found, these need to be removed
            if len(i.text) != 0:
                recos.append(i.get_attribute('href').replace('https://www.youtube.com/watch?v=', ''))
        self.get_video_features(source, recos)
        return recos[0:branching]

    def shutdown(self):
        self.driver.quit()