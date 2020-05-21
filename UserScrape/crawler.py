
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementNotInteractableException, ElementNotVisibleException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver
from datetime import datetime
import time
import os
import uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from pathlib import Path
import json
from urllib.parse import urlparse
from dataclasses import dataclass
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath, WindowsPath
import tempfile
import asyncio
import discord_bot
from typing import List


@dataclass
class CrawlResult:
    success: bool = True
    res: str = None


def create_driver(headless: bool) -> WebDriver:
    options = Options()
    if(headless):
        options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    # to load more recommendations on the feed
    options.add_argument("--window-size=1920,1080")
    # this is mark@ledwich.com's recently used user agent.
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36")
    capabilities = DesiredCapabilities.CHROME.copy()
    capabilities['acceptSslCerts'] = True
    capabilities['acceptInsecureCerts'] = True
    return webdriver.Chrome(options=options, desired_capabilities=capabilities)


class Crawler:
    def __init__(self, data_storage_cs:str, email:str, password:str, tel_nr:str, headless:bool, lang = 'en'):
        self.container = ContainerClient.from_connection_string(data_storage_cs, "userscrape")
        self.driver = create_driver(headless)
        self.wait = WebDriverWait(self.driver, 10)
        self.email = email
        self.password = password
        self.tel_nr = tel_nr
        self.init_time = datetime.now()
        self.lang = lang
        self.trial_nr = 1

    def test_ip(self):
        wd = self.driver
        wd.get('https://httpbin.org/ip')
        pre: WebElement = wd.find_element_by_css_selector('pre')
        print(f'Running with IP {json.loads(pre.text)["origin"]}')
        self.__log_info('ip')

    def load_home_and_login(self):
        wd = self.driver
        # need to go to the domain to add cookies
        wd.get('https://www.youtube.com')
        self.__load_cookies()

        wd.get('https://www.youtube.com')
        content = WebDriverWait(wd, 10).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, '#contents')))

        self.__log_info('home')

        try:
            login = wd.find_element_by_css_selector(
                'paper-button[aria-label="Sign in"]')
        except NoSuchElementException:
            login = None

        if(login != None):
            self.login()

    def login(self) -> CrawlResult:
        wd = self.driver

        # this link is maybe too specific (e.g. it contains country codes)
        wd.get(
            f'https://accounts.google.com/signin/v2/identifier?service=youtube&uilel=3&passive=true&continue=https%3A%2F%2Fwww.youtube.com%2Fsignin%3Faction_handle_signin%3Dtrue%26app%3Ddesktop%26hl%3D{self.lang}%26next%3D%252F&hl={self.lang}&ec=65620&flowName=GlifWebSignIn&flowEntry=ServiceLogin')

        emailEl: WebElement = WebDriverWait(wd, 5).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, 'input[type="email"]'))
        )
        self.__log_info(f'enter_email')
        emailEl.send_keys(self.email)
        # next_button = wd.find_element_by_id('next').click()
        wd.find_element_by_css_selector('#identifierNext').click()

        passwordEl: WebElement = WebDriverWait(wd, 5).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, 'input[type="password"]'))
        )
        self.__log_info('email_entered')
        passwordEl.send_keys(self.password)
        passwordUrl = wd.current_url
        wd.find_element_by_css_selector('#passwordNext').click()

        self.__log_info('password_entered')

        time.sleep(1)
        phone_input = self.driver.find_elements_by_xpath("//input[@type='tel']")
        revalidation = self.driver.find_elements_by_xpath("//*[@data-sendmethod='SMS']")
        if len(phone_input) != 0:
            phone_input[0].send_keys(self.tel_nr)
            self.__log_info('phone_number_entered')
            tel_next_button = self.driver.find_element_by_id('idvanyphonecollectNext').click()
            code_bot = discord_bot.DiscordBot()
            code = code_bot.get_code()
            code_input = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//input[@type='tel']"))
            ).send_keys(code)
            self.__log_info('code_entered')
            code_next_button = self.driver.find_element_by_id('idvanyphoneverifyNext').click()
        elif len(revalidation) != 0:
            # revalidation
            revalidation.click()  # select sms option
            code_bot = discord_bot.DiscordBot()
            code = code_bot.get_code()
            code_input = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//input[@type='tel']"))
            ).send_keys(code)
            self.__log_info('code_entered')
            code_next_button = self.driver.find_element_by_id('idvanyphoneverifyNext').click()
        feed = self.wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="grid-title"]')))
        self.__log_info('home')
        self.__save_cookies()

        return CrawlResult()

    def get_video_features(self, videoId, recommendations: list, personalized_count: int):
        seshPath = self.path_session()
        filename = 'output/recommendations/' + self.email + '_' + videoId + '_' + \
            str(self.init_time).replace(':', '-').replace(' ', '_') + '.json'
        
        video_info = {
            'account': self.email,
            'trial': self.trial_nr,
            'id': videoId,
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

        # upload the information as a blob
        self.__save_file(seshPath / filename, str(video_info))

    def get_recommendations_for_video(self, source):
        self.driver.get("https://www.youtube.com/watch?v=" + source)

        # this is the list of elements from the recommendation sidebar
        # it does not always load all recommendations at the same time, therefore the loop
        all_recs = []
        while len(all_recs) < 19:
            all_recs = self.wait.until(
                EC.visibility_of_all_elements_located(
                    (By.XPATH, '//*[@id="dismissable"]/div/div[1]/a'))
            )

        recos = []
        personalized_counter = 0 # how many of the recommendations are personalized?
        for i in all_recs:
            personalized = 'Recommended for you' in i.text
            if personalized:
                personalized_counter += 1
            # take the link and remove everything except for the id of the video that the link leads to
            recommendation_id = i.get_attribute('href').replace(
                'https://www.youtube.com/watch?v=', '')
            title = i.find_element_by_xpath('//*[@id="video-title"]').get_attribute('title')
            full_info = i.find_element_by_xpath('//*[@id="video-title"]').get_attribute('aria-label')
            recos.append({
                'id': recommendation_id,
                'personalized': personalized,
                'title': title,
                'full_info': full_info})
        # store the information about the current video plus the corresponding recommendations
        self.get_video_features(source, recos, personalized_counter)
        # return the recommendations 
        return recos

    def delete_last_video_from_history(self, video_id: str):
        self.driver.get('https://www.youtube.com/feed/history')
        # self.__log_info(f'before_deleting_last_video_{video_id}')
        first_video = self.wait.until(
            EC.presence_of_element_located((By.XPATH,
                                            '//*[@id="video-title"]'))
        )
        # the link might contain a time stamp so we we need to use split to only get the video id
        first_video_id = first_video.get_attribute('href').replace('https://www.youtube.com/watch?v=', '').split('&')[0]
        delete_buttons = self.wait.until(
                EC.presence_of_all_elements_located(
                    (By.XPATH, "//*[@aria-label = 'Remove from Watch history']"))
            )
        # delete_buttons = self.driver.find_elements_by_xpath("//*[@aria-label = 'Aus \"Wiedergabeverlauf\" entfernen']")
        
        # reasons why there are no videos in the history:
        # 1. the history is empty
        # 2. we are actually not logged in
        # 3. The ui is in the wrong language
        if len(delete_buttons)>0 and first_video_id == video_id: # checking if the most recent video is actually the video we want to delete 
            delete_buttons[0].click()
        # self.__log_info(f'after_deleting_last_video_{video_id}')

    def delete_history(self):
        self.driver.get('https://www.youtube.com/feed/history')
        # self.__log_info('before_history_deletion')
        messages = self.driver.find_elements_by_xpath("//*[@id='message']")
        # if there are not videos in the history a text appears that says 'no videos here' but apparently there is a second, hidden, message with the same
        # id on the page. So instead of checking whether this element exists we differentiate between 1 message (there are videos in the history) and 
        # two messages (there are no videos in the history)
        if len(messages)==1:
            delete_buttons =  self.wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, "//*[@aria-label = 'Clear all watch history']")) 
            ).click()
            # delete_buttons = self.driver.find_element_by_xpath("//*[@aria-label = 'Gesamten Wiedergabeverlauf löschen']").click()

            confirm_button = self.wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, "//*[@aria-label = 'CLEAR WATCH HISTORY']"))
            ).click()
            # confirm_button = self.driver.find_element_by_xpath("//*[@aria-label = 'WIEDERGABEVERLAUF LÖSCHEN']").click()

        # self.__log_info('after_history_deletion')

    def _get_seconds(self, duration: str):
        # helper function to correctly parse the time from the info bar
        if duration == '':
            print("Duration of video couldn't be read")
            return 0
        if len(duration) > 5:
            duration_time = datetime.strptime(duration, "%H:%M:%S")
        else:
            duration_time = datetime.strptime(duration, "%M:%S")
        return (duration_time-datetime(1900, 1, 1)).total_seconds()

    async def watch_video(self, videoId: str, main_tab: str, current_tab: str):
        """[summary]
        starts video, skips any ads in the beginning and then watches video for an amount of time that is dependent on video length
        Currently we are ignoring ads that appear in the middle of the video. At the moment we are assuming that watching an ad also contributes
        to the watchtime of the video itself

        Arguments:
            videoId {str} -- id of video
            main_tab {str} -- the selenium window handle of the main tab to switch back to after current tab is closed
            current_tab {str} -- the tab in which the video shall be watched
        """
        seshPath = self.path_session()

        self.driver.switch_to.window(current_tab)
        self.driver.get("https://www.youtube.com/watch?v=" + videoId)
        # wait until video is loaded
        playbutton = self.wait.until(
            EC.presence_of_element_located((By.XPATH,
                                            '//*[@class="ytp-play-button ytp-button"]'))
        )
        # todo: in the end this will be only english
        if playbutton.get_attribute('title') in ["Play (k)", "Wiedergabe (k)"]:
            playbutton.click()
        # unfortunately the ad loads slower than the player so we wait here to be sure we detect the ad if any appears
        time.sleep(1)
        # we store any advertisements that appear
        advertisements = {videoId: []}
        filename = 'output/advertisements/' + self.email + '_' + videoId + '_' + \
            str(self.init_time).replace(':', '-').replace(' ', '_') + '.json'
        # self.__log_info(f"{videoId}_opened")
        # we check whether a skip button is present
        if len(self.driver.find_elements_by_xpath("//*[@class='ytp-ad-preview-container countdown-next-to-thumbnail']")) != 0:
            # store the advertiser
            advertisements[videoId].append(self.driver.find_element_by_xpath(
                "//*[@class='ytp-ad-button ytp-ad-visit-advertiser-button ytp-ad-button-link']").text)
            # wait until we can skip
            time.sleep(5)
            # if the ad is only 5 seconds long, it is already over now, so have to check whether the button is still there before we click it
            skip_button = self.driver.find_element_by_xpath("//*[@class='ytp-ad-skip-button ytp-button']")
            if skip_button.is_displayed():
                skip_button.click()
        # wait again, for second ad that might appear
        time.sleep(1)
        # check for presence of second ad
        if len(self.driver.find_elements_by_xpath("//*[@class='ytp-ad-preview-container countdown-next-to-thumbnail']")) != 0:
            advertisements[videoId].append(self.driver.find_element_by_xpath(
                "//*[@class='ytp-ad-button ytp-ad-visit-advertiser-button ytp-ad-button-link']").text)
            time.sleep(5)
            skip_button = self.driver.find_element_by_xpath("//*[@class='ytp-ad-skip-button ytp-button']")
            if skip_button.is_displayed():
                skip_button.click()

        # measure for how long we are watching the actual video
        start_time = time.time()

        # upload the list of advertisers
        self.__save_file(seshPath / filename, str(advertisements))

        # detect the length of th actual video
        time_element = WebDriverWait(self.driver, 3).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'ytp-time-duration')))
        ActionChains(self.driver).move_to_element(time_element).perform() # we need to hover over the bar or else the time is not visible
        duration = time_element.text
        duration = self._get_seconds(duration)
        # to make sure that every video is watched long enough
        # watch_time = duration if duration < 300 else 300 if duration/3 < 300 else duration/3
        watch_time = duration if duration < 300 else 300 
        print(watch_time)
        # let the asynchronous manager know that now other videos can be started
        await asyncio.sleep(watch_time)
        #todo replace with seq logging
        watch_time_log_file = 'output/watch_times/' + self.email + '_' + videoId + '_' + \
            str(self.init_time).replace(':', '-').replace(' ', '_') + '.json'
        watch_time = {
            'account': self.email,
            'trial': self.trial_nr,
            'video_id': videoId,
            'video_length': duration,
            'goal_watch_time': watch_time,
            'watch_time': time.time()-start_time
        }
        self.__save_file(seshPath / watch_time_log_file, str(watch_time))
        self.driver.switch_to.window(current_tab)
        # self.__log_info(f'{videoId}_watched')
        self.driver.close()
        self.driver.switch_to.window(main_tab)

    async def watch_videos(self, videos: List[str]):
        """
        This methods starts watching multiple videos in different tabs asynchronously, I.e. while one watch_video method is in the
        state of just sleeping, it already opens the next tab with another video.
        As soon as video is finished watching, the tab is closed

        Arguments:
            videos {list[str]} -- a list with all the video id's that are supposed to be watched
        """            
        tasks = []
        main_window = self.driver.window_handles[-1]
        for video in videos:
            self.driver.execute_script("window.open('');")
            new_tab = self.driver.window_handles[-1]
            tasks.append(
                self.watch_video(video, main_window, new_tab)
            )
        await asyncio.gather(*tasks)

    def scan_feed(self):
        seshPath = self.path_session()
        
        # especially during the corona crisis, YouTube is offering a lot of extra information
        # they add noise to our data aquisition, because they influence how many videos are shown
        # on the home page, so we have to get rid of them
        # if we close these extra sections, YouTube remembers and doesnt show them again
        # ideally, this loop is only run once per account
        feed_is_bannerfree = False
        while not feed_is_bannerfree:
            # refresh the feed everytime we had to close something until we finally get a completely clean feed
            self.driver.get("https://www.youtube.com")
            # set the stop condition to True unless any 'banners' are detected
            feed_is_bannerfree = True
            try:
                # this is the link to the WHO
                # there are 3 other buttons with the same aria-label, which cannot be clicked, so I simply try to click them all and catch the exception
                # unfortunately there is no other way to uniquely identify that button
                extra_content = WebDriverWait(self.driver, 2).until(  # Schließen
                    EC.presence_of_all_elements_located((By.XPATH, '//*[@aria-label="Close"]')))
                for button in extra_content:
                    try:
                        button.click()
                        feed_is_bannerfree = False
                        print("information closed")
                    except ElementNotInteractableException:
                        pass
                    except ElementNotVisibleException:
                        pass
            except TimeoutException:
                # print("no extra covid information")
                pass

            # these kinds of banners are partly corona specific (like #fitnessathome) or not (#trendingmovies)
            themed_content = None
            try:
                themed_content = WebDriverWait(self.driver,2).until(  # Kein Interesse
                    EC.presence_of_all_elements_located((By.XPATH, '//*[@aria-label="Not interested"]')))
            except TimeoutException:
                # print("No themed content")
                pass

            if themed_content is not None:
                feed_is_bannerfree = False
                for button in themed_content:
                    button.click()
                    print("banner closed")

        all_videos = self.wait.until(
            EC.presence_of_all_elements_located((By.XPATH, '//*[@id="video-title-link"]'))
        )

        feed_info = dict(
            account = self.email,
            trial = self.trial_nr,
            feed_videos = []
        )
        for video in all_videos:
            # take the link and remove everything except for the id of the video that the link leads to
            vid_dict = dict(
                vid_id = video.get_attribute('href').replace('https://www.youtube.com/watch?v=', ''),
                title = video.get_attribute('title'),
                full_info = video.get_attribute('aria-label')
            )
            feed_info['feed_videos'].append(vid_dict)

        filename = 'output/feed/' + self.email + '_' + \
        str(self.init_time).replace(':', '-').replace(' ', '_') + '.json'
        # upload the information as a blob
        self.__save_file(seshPath / filename, str(feed_info))
        

    def __save_cookies(self):
        """saves all cookies
        """
        cookies = {'cookies': self.driver.get_cookies()}
        self.__save_file(self.path_user() / 'cookies.json',
                         json.dumps(cookies))

    def __load_cookies(self):
        """loads cookies for the current domain
        """
        cookiePath = self.path_user() / 'cookies.json'

        try:
            blob = self.container.download_blob(cookiePath.as_posix())
        except BaseException as e:
            blob = None

        if(blob == None):
            return
        currentUrl = urlparse(self.driver.current_url)
        for c in json.loads(blob.content_as_text())['cookies']:
            if currentUrl.netloc.endswith(c['domain']):
                # not sure why, but this stops it being loaded.
                c.pop('expiry', None)
                try:
                    self.driver.add_cookie(c)
                except BaseException as e:
                    print(f'could not load cookies from: {cookiePath}: {e}')

    # easy method to save screenshots for headless mode
    def __log_info(self, name: str):
        wd = self.driver

        seshPath = self.path_session()

        # save page source
        self.__save_file(seshPath / f'{name}.html', wd.page_source)

        # save metadata
        state = {
            'url': wd.current_url,
            'title': wd.title
        }
        self.__save_file(seshPath / f'{name}.json', json.dumps(state))

        # save image
        imagePath = seshPath / f'{name}.png'
        localImagePath = Path(tempfile.gettempdir()) / imagePath
        wd.get_screenshot_as_file(str(localImagePath))
        self.__upload_file(localImagePath, imagePath)

        print(f'scraped: {name} - {seshPath}')

    def __save_file(self, relativePath: PurePath, content: str):

        localPath = Path(tempfile.gettempdir()) / relativePath
        localPath.parent.mkdir(parents=True, exist_ok=True)
        with open(localPath, "w", encoding="utf-8") as w:
            w.write(content)
        self.__upload_file(localPath, relativePath)

    def __upload_file(self, localFile: PurePath, remotePath: PurePath):
        with open(localFile, 'rb') as f:
            self.container.upload_blob(
                remotePath.as_posix(), f, overwrite=True)

    def path_user(self)-> PurePath: 
        return PurePosixPath(f'session_logs/{self.email}')

    def path_session(self) -> PurePath:
        return PurePosixPath(f'session_logs/{self.email}/{self.init_time.strftime("%Y%m%d-%H%M%S")}.trial_{self.trial_nr}')

    def update_trial(self):
        self.trial_nr += 1

    def shutdown(self):
        self.driver.quit()
