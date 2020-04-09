
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from pathlib import Path
from time import sleep
import json
from urllib.parse import urlparse
from dataclasses import dataclass

@dataclass
class CrawlResult:
    success: bool = True
    res: str = None

class Crawler:
    def __init__(self, driver:WebDriver, sas_url:str, email:str, password:str, lang = 'en'):
        self._video_infos = {}
        self.wait = WebDriverWait(driver, 10)
        self.driver = driver
        self.container = ContainerClient.from_container_url(sas_url)
        self.email = email
        self.password = password
        self.init_time = datetime.now()
        self.lang = lang


    def load_home_and_login(self):
        wd = self.driver
        wd.get('https://www.youtube.com') # need to go to the domain to add cookies
        self.__load_cookies()

        wd.get('https://www.youtube.com')
        content = WebDriverWait(wd, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, '#contents')))

        self.__log_info('home')
        
        try:
            login = wd.find_element_by_css_selector('paper-button[aria-label="Sign in"]')
        except NoSuchElementException:
            login = None

        if(login != None):
            self.login()

    def login(self) -> CrawlResult:
        wd = self.driver
        

         # this link is maybe too specific (e.g. it contains country codes)
        wd.get(f'https://accounts.google.com/signin/v2/identifier?service=youtube&uilel=3&passive=true&continue=https%3A%2F%2Fwww.youtube.com%2Fsignin%3Faction_handle_signin%3Dtrue%26app%3Ddesktop%26hl%3D{self.lang}%26next%3D%252F&hl={self.lang}&ec=65620&flowName=GlifWebSignIn&flowEntry=ServiceLogin')

        emailEl:WebElement = WebDriverWait(wd, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[type="email"]'))
        )
        self.__log_info(f'enter_email')
        emailEl.send_keys(self.email)
        wd.find_element_by_css_selector('#identifierNext').click() #next_button = wd.find_element_by_id('next').click()

        passwordEl:WebElement = WebDriverWait(wd, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[type="password"]'))
        )
        self.__log_info('email_entered')
        passwordEl.send_keys(self.password)
        passwordUrl = wd.current_url
        wd.find_element_by_css_selector('#passwordNext').click()

        sleep(1)
        self.__log_info('password_entered')

        url = urlparse(wd.current_url)
        if url.netloc != "www.youtube.com":
            verify = wd.find_element_by_css_selector('#authzenNext')
            if(verify):
                verify.click()

                # verify, at least on my account presents a number to enter on the phone
                figure:WebElement = WebDriverWait(wd, 2).until(
                    EC.text_to_be_present_in_element((By.CSS_SELECTOR, 'figure > samp'))
                    )
                    
                self.sendMessageToUser(f'Enter {figure.text} on your phone')

                # wait for 5 minutes for an IRL meat-person to verify
                WebDriverWait(wd, 5*60).until(EC.url_changes(wd.current_url))
                newUrl = urlparse(wd.current_url)
                if newUrl.netloc != "youtube.com":
                    return CrawlResult(True, f'did not navigate to youtube after verifying (url:{wd.current_url})')

            return CrawlResult(True, f'did not nvagate to youtube after password (url:{wd.current_url})')

        self.__save_cookies()

        return CrawlResult()
        

    def sendMessageToUser(self, message):
        #todo send to discour/slack/email to get the meat-user to click a number
        print(f'to {self.emial}: {message}')

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

    def __save_cookies(self):
        cookies = { 'cookies': self.driver.get_cookies() }
        self.__save_file(f'{self.path_user()}/cookies.json', json.dumps(cookies))

    def __load_cookies(self):
        cookiePath = f'{self.path_user()}/cookies.json'
        try:
            blob = self.container.download_blob(cookiePath)
            for c in json.loads(blob.content_as_text())['cookies']:
                self.driver.add_cookie(c)
        except:
            print(f'could not load cookies from: {cookiePath}')


        # easy method to save screenshots for headless mode
    def __log_info(self, name:str):
        wd = self.driver

        seshPath = self.path_session()

        self.__save_file(f'{seshPath}/{name}.html', wd.page_source)

        state = {
            'url':wd.current_url,
            'title':wd.title
        }
        self.__save_file(f'{seshPath}/{name}.json', json.dumps(state))
        
        imagePath = f'{seshPath}/{name}.png'
        localImagePath =  f'/tmp/{imagePath}'
        wd.get_screenshot_as_file(localImagePath)
        self.__upload_file(localImagePath, imagePath)

        print(f'scraped: {name} - {seshPath}')


    def __save_file(self, relativePath:str, content:str):
        localPath = f'/tmp/{relativePath}'
        Path(localPath).parent.mkdir(parents=True, exist_ok=True)
        with open(localPath, "w") as w:
            w.write(content)
        self.__upload_file(localPath, relativePath)


    def __upload_file(self, localFile, remotePath):
        with open(localFile, 'rb') as f:
            self.container.upload_blob(remotePath, f, overwrite=True)

    def path_user(self):
        return f'user_scrape/{self.email}'

    def path_session(self):
        return f'user_scrape/{self.email}/{self.init_time.strftime("%Y%m%d-%H%M%S")}.{self.driver.session_id}'

    def shutdown(self):
        self.driver.quit()



    