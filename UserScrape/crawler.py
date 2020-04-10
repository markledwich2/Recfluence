
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from datetime import datetime
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from pathlib import Path
import json
from urllib.parse import urlparse
from dataclasses import dataclass
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath, WindowsPath
import tempfile

@dataclass
class CrawlResult:
    success: bool = True
    res: str = None

def create_driver(headless:bool) -> WebDriver:
    options = Options()
    if(headless):
        options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--window-size=1920,1080") #to load more recommendations on the feed
    # this is mark@ledwich.com's recently used user agent.
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36")
    capabilities = DesiredCapabilities.CHROME.copy()
    capabilities['acceptSslCerts'] = True
    capabilities['acceptInsecureCerts'] = True
    return webdriver.Chrome(options=options, desired_capabilities=capabilities)

class Crawler:
    def __init__(self, sas_url:str, email:str, password:str, headless:bool, lang = 'en'):
        self._video_infos = {}
        self.driver = create_driver(headless)
        self.wait = WebDriverWait(self.driver, 10)
        self.container = ContainerClient.from_container_url(sas_url)
        self.email = email
        self.password = password
        self.init_time = datetime.now()
        self.lang = lang

    def test_ip(self):
        wd = self.driver
        wd.get('https://httpbin.org/ip')
        pre:WebElement = wd.find_element_by_css_selector('pre')
        print(f'Running with IP {json.loads(pre.text)["origin"]}')

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
        print(f'to {self.email}: {message}')

    def get_video_features(self, id, recommendations, personalized_count):
        filename = 'output/recommendations/' + self.email +'_'+ id +'_'+ str(self.init_time).replace(':','-').replace(' ','_') + '.json'
        video_info = {
            'id': id,
            'title': self.wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#container > h1 > yt-formatted-string"))).text,
            'channel': self.wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR,
                "ytd-channel-name.ytd-video-owner-renderer > div:nth-child(1) > "
                "div:nth-child(1)"))).text,
            'channel_id': self.wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#text > a"))).get_attribute('href').strip(
                'https://www.youtube.com/channel/'),
            'recommendations': recommendations,
            'personalization_count': personalized_count
        }

        self.__save_file(filename, str(video_info))
        # self.__upload_file(filename, filename)

    def get_recommendations_for_video(self, source):
        self.driver.get("https://www.youtube.com/watch?v=" + source)

        # this is the list of elements from the recommendation sidebar
        # it does not always load all recommendations at the same time, therefore the loop
        all_recs = []
        while len(all_recs) < 19:
            all_recs = WebDriverWait(self.driver, 10).until(
                EC.visibility_of_all_elements_located((By.XPATH, '//*[@id="dismissable"]/div/div[1]/a'))
            )

        recos = {}
        personalized_counter = 0
        for i in all_recs:
            personalized = 'Recommended for you' in i.text
            if personalized:
                personalized_counter += 1
            # take the link and remove everything except for the id of the video that the link leads to
            recommendation_id = i.get_attribute('href').replace('https://www.youtube.com/watch?v=', '')
            recos[recommendation_id] = {'id': recommendation_id, 'personalized': personalized}
        # store the information about the current video plus the corresponding recommendations
        self.get_video_features(source, recos, personalized_counter)
        # return a number of recommendations up to the branching factor
        return recos

    def delete_last_video_from_history(self):
        self.driver.get('https://www.youtube.com/feed/history')
        # todo: maybe replace the full xpath because its ugly, but it works
        last_video_delete_button = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.XPATH,
                                            '/html/body/ytd-app/div/ytd-page-manager/ytd-browse/ytd-two-column-browse'
                                            '-results-renderer/div[1]/ytd-section-list-renderer/div['
                                            '2]/ytd-item-section-renderer[1]/div[3]/ytd-video-renderer/div['
                                            '1]/div/div['
                                            '1]/div/div/ytd-menu-renderer/div/ytd-button-renderer/a/yt-icon-button'
                                            '/button'))
        ).click()


    def delete_history(self):
        self.driver.get('https://www.youtube.com/feed/history')
        history_delete_button = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.XPATH,
                                            '/html/body/ytd-app/div/ytd-page-manager/ytd-browse/ytd-two-column-browse'
                                            '-results-renderer/div['
                                            '2]/ytd-browse-feed-actions-renderer/div/ytd-button-renderer['
                                            '1]/a/paper-button/yt-formatted-string'))
        ).click()

        confirm_button = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.XPATH,
                                            '/html/body/ytd-app/ytd-popup-container/paper-dialog/yt-confirm-dialog'
                                            '-renderer/div[2]/div/yt-button-renderer['
                                            '2]/a/paper-button/yt-formatted-string'))
        ).click()

    def __save_cookies(self):
        """saves all cookies
        """
        cookies = { 'cookies': self.driver.get_cookies() }
        self.__save_file(self.path_user() / 'cookies.json', json.dumps(cookies))

    def __load_cookies(self):
        """loads cookies for the current domain
        """
        cookiePath = self.path_user() / 'cookies.json'
        
        try:
            blob = self.container.download_blob(cookiePath.as_posix())
        except BaseException as e:
            blob = None

        if(blob == None): return
        currentUrl = urlparse(self.driver.current_url)
        for c in json.loads(blob.content_as_text())['cookies']:
            if currentUrl.netloc.endswith(c['domain']):
                c.pop('expiry', None) # not sure why, but this stops it being loaded.
                try:
                    self.driver.add_cookie(c)
                except BaseException as e:
                    print(f'could not load cookies from: {cookiePath}: {e}')

    # easy method to save screenshots for headless mode
    def __log_info(self, name:str):
        wd = self.driver

        seshPath = self.path_session()

        # save page source
        self.__save_file(seshPath / f'{name}.html', wd.page_source)

        # save metadata
        state = {
            'url':wd.current_url,
            'title':wd.title
        }
        self.__save_file(seshPath / f'{name}.json', json.dumps(state))
        
        # save image
        imagePath = seshPath / f'{name}.png'
        localImagePath =  Path(tempfile.gettempdir()) / imagePath
        wd.get_screenshot_as_file(str(localImagePath))
        self.__upload_file(localImagePath, imagePath)

        print(f'scraped: {name} - {seshPath}')


    def __save_file(self, relativePath:PurePath, content:str):

        localPath = Path(tempfile.gettempdir()) / relativePath
        localPath.parent.mkdir(parents=True, exist_ok=True)
        with open(localPath, "w", encoding="utf-8") as w:
            w.write(content)
        self.__upload_file(localPath, relativePath)


    def __upload_file(self, localFile:PurePath, remotePath:PurePath):
        with open(localFile, 'rb') as f:
            self.container.upload_blob(remotePath.as_posix(), f, overwrite=True)

    def path_user(self)-> PurePath: 
        return PurePosixPath(f'user_scrape/{self.email}')

    def path_session(self) -> PurePath:
        return PurePosixPath(f'user_scrape/{self.email}/{self.init_time.strftime("%Y%m%d-%H%M%S")}.{self.driver.session_id}')

    def shutdown(self):
        self.driver.quit()



    