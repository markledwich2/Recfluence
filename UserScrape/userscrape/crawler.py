
from more_itertools.more import difference
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import ElementNotInteractableException, ElementNotVisibleException, NoSuchElementException, TimeoutException, WebDriverException
#from selenium.webdriver.chrome.options import Options
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver
from datetime import datetime
import time
import os
from pathlib import Path
import json
from urllib.parse import urlparse
from dataclasses import asdict, dataclass
from dataclasses_json import dataclass_json
from pathlib import Path
import tempfile
import asyncio
from typing import Any, List, Dict
from .store import BlobStore, BlobPaths, file_date_str
from .discord_bot import DiscordBot
from .cfg import Cfg, UserCfg
import logging
from logging import Logger
from more_itertools import chunked


@dataclass_json
@dataclass
class CrawlResult:
    success: bool = True
    res: str = None


@dataclass_json
@dataclass
class VideoUnavailable:
    reason: str
    sub_reason: str


@dataclass_json
@dataclass
class RecResult:
    recs: List[Dict[str, Any]]
    unavailable: VideoUnavailable = None


class DetectedAsBotException(Exception):
    pass


def create_firefox_driver(headless: bool) -> WebDriver:
    options = webdriver.firefox.options.Options()
    options.headless = headless
    return webdriver.Firefox(options=options)


def create_chrome_driver(headless: bool) -> WebDriver:
    options = webdriver.chrome.options.Options()
    if(headless):
        options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    # options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-extensions")
    # to load more recommendations on the feed
    options.add_argument("--window-size=1920,1080")
    # this is mark@ledwich.com's recently used user agent.
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36")
    capabilities = DesiredCapabilities.CHROME.copy()
    capabilities['acceptSslCerts'] = True
    capabilities['acceptInsecureCerts'] = True
    # service_args=["--verbose", "--log-path=/var/log/chromedriver.log"]
    return webdriver.Chrome(options=options, desired_capabilities=capabilities)


class Crawler:
    def __init__(self, store: BlobStore, bot: DiscordBot, user: UserCfg, cfg: Cfg, trial_id: str, log: Logger, lang='en'):
        self.store = store
        self.bot = bot
        self.driver = create_chrome_driver(
            cfg.headless) if cfg.browser == 'chrome' else create_firefox_driver(cfg.headless)
        self.wait = WebDriverWait(self.driver, 10)
        self.user = user
        self.init_time = datetime.now()
        self.log = log
        self.lang = lang
        self.trial_id = trial_id
        self.max_watch_secs = cfg.max_watch_secs
        self.videos_parallel = cfg.videos_parallel
        self.session_id = file_date_str()
        self.path = BlobPaths(store.cfg, trial_id, user, self.session_id)

    async def test_ip(self):
        wd = self.driver
        wd.get('https://httpbin.org/ip')
        pre: WebElement = wd.find_element_by_css_selector('pre')
        print(f'Running with IP {json.loads(pre.text)["origin"]}')
        self.__log_driver_status('ip')

    async def load_home_and_login(self):
        wd = self.driver
        # need to go to the domain to add cookies
        wd.get('https://www.youtube.com')
        self.__load_cookies()

        wd.get('https://www.youtube.com')
        self.wait_for_visible('#contents', timeout=10)
        self.__log_driver_status('home')

        try:
            login = wd.find_element_by_css_selector('paper-button[aria-label="Sign in"]')
        except NoSuchElementException:
            login = None

        if(login != None):
            await self.login()

    def wait_for_visibles(self, cssSelector: str, error_expected: bool = False) -> WebElement:
        """waits for all elements to be visible  at the given css selector, logs errors to seq & discord"""
        try:
            return WebDriverWait(self.driver, 5).until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, cssSelector)))
        except WebDriverException as e:
            self.handle_driver_ex(e, cssSelector, error_expected)
        raise e

    def wait_for_visible(self, cssSelector: str, error_expected: bool = False, timeout: int = 5) -> WebElement:
        """waits for an element to be visible  at the given css selector, logs errors to seq & discord"""
        try:
            return WebDriverWait(self.driver, timeout).until(EC.visibility_of_element_located((By.CSS_SELECTOR, cssSelector)))
        except WebDriverException as e:
            self.handle_driver_ex(e, cssSelector, error_expected)
        raise e

    def wait_for_clickable(self, cssSelector: str, error_expected: bool = False) -> WebElement:
        """waits for an element to be clickable at the given css selector, logs errors to seq & discord"""
        try:
            return WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, cssSelector)))
        except WebDriverException as e:
            self.handle_driver_ex(e, cssSelector, error_expected)

    def handle_driver_ex(self, e: WebDriverException, selector: str, expected: bool = False):
        ex_name = e.__class__.__name__
        if(expected):
            self.log.debug('selector {selector} failed with {ex_name} (but we expected it to)',
                           selector=selector, ex_name=ex_name)
        else:
            self.__log_driver_status(selector, ex_name)
        raise e

    async def login(self) -> CrawlResult:
        wd = self.driver
        user = self.user

        # this link is maybe too specific (e.g. it contains country codes)
        wd.get(
            f'https://accounts.google.com/signin/v2/identifier?service=youtube&uilel=3&passive=true&continue=https%3A%2F%2Fwww.youtube.com%2Fsignin%3Faction_handle_signin%3Dtrue%26app%3Ddesktop%26hl%3D{self.lang}%26next%3D%252F&hl={self.lang}&ec=65620&flowName=GlifWebSignIn&flowEntry=ServiceLogin')

        wfc = self.wait_for_clickable
        wfv = self.wait_for_visible

        def onHome():
            wfv(homeSelector)
            self.__save_cookies()

        wfc('input[type="email"]').send_keys(user.email)
        # next_button = wd.find_element_by_id('next').click()
        wfc('#identifierNext').click()
        wfc('input[type="password"]').send_keys(user.password)
        wfc('#passwordNext').click()
        await asyncio.sleep(2)

        telSelector = 'input[type="tel"]'
        smsSelector = '*[data-sendmethod="SMS"]'
        captchaSelector = 'input[aria-label="Type the text you hear or see"]'
        homeSelector = '#primary'

        authEl: WebElement = wfv(f'{telSelector}, {smsSelector}, {captchaSelector}, {homeSelector}')
        if authEl.get_attribute('id') == 'primary':
            onHome()
            return CrawlResult()
        if authEl.get_attribute('type') == 'tel':
            wfc(telSelector).send_keys(user.telephone_number)
            wfc('#idvanyphonecollectNext').click()
            code = await self.bot.request_code(user)
            wfc(telSelector).send_keys(code)
            wfc('#idvanyphoneverifyNext').click()
        elif authEl.get_attribute('data-sendmethod') == 'SMS':
            wfc(smsSelector).click()  # select sms option
            code = await self.bot.request_code(user)
            wfc(telSelector).send_keys(code)
            wfc('#idvPreregisteredPhoneNext').click()
        elif authEl.get_attribute('aria-label') == 'Type the text you hear or see':
            captchaPath = self.__save_image('captcha')
            captcha = await self.bot.request_code(user, "enter the catpcha", captchaPath)
            wfc(captchaSelector).send_keys(captcha)
            wfc('#identifierNext').click()
        else:
            raise WebDriverException('unable to find post-password element')

        onHome()
        return CrawlResult()

    def get_video_features(self, video_id, rec_result: RecResult):
        available = rec_result.unavailable == None
        video_info = {
            'account': self.user.tag,
            'trial': self.trial_id,
            'video_id': video_id,
            'title': self.wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#container > h1 > yt-formatted-string"))).text if available else None,
            'channel': self.wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR,
                 "ytd-channel-name.ytd-video-owner-renderer > div:nth-child(1) > "
                 "div:nth-child(1)"))).text if available else None,
            'channel_id': self.wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#text > a"))).get_attribute('href').strip(
                'https://www.youtube.com/channel/') if available else None,  # read this from our own data if we have it. unavailable video's don't show the channel
            'recommendations': rec_result.recs if available else None,
            'unavailable': asdict(rec_result.unavailable) if rec_result.unavailable else None,
            'updated': datetime.utcnow().isoformat()
        }
        self.store.save(self.path.rec_json(video_id), video_info)

    async def get_recommendations_for_video(self, video_id) -> bool:
        """collects recommendations for a video.

        Returns:
            bool -- true if the video was watched, false if it was skipped
        """

        rec_path = self.path.rec_json(video_id)
        rec_result = self.store.load_dic(rec_path)
        if(rec_result):
            self.log.info('{tag} -skipping recommendation {video}', tag=self.user.tag, video=video_id)
            return False

        self.log.debug('{tag} - about load video page for recs {video}', tag=self.user.tag, video=video_id)
        self.driver.get("https://www.youtube.com/watch?v=" + video_id)
        self.log.debug('{tag} - loaded video page for recs {video}', tag=self.user.tag, video=video_id)

        url = urlparse(self.driver.current_url)
        if(url.path == '/sorry/index'):  # the sorry we think you are a bot page ()
            self.log.debug('{tag} - raising we have been redirected to the you-are-a-bot-page', tag=self.user.tag)
            raise DetectedAsBotException('we have been redirected to the you-are-a-bot-page')

        # this is the list of elements from the recommendation sidebar
        # it does not always load all recommendations at the same time, therefore the loop
        all_recs = []
        unavalable: VideoUnavailable = None
        findRecsEx: WebDriverException = None
        rec_attempt = 0
        while True:
            try:
                all_recs = self.driver.execute_script('''
 return (ytInitialData.contents.twoColumnWatchNextResults ?? 
 document.querySelector('ytd-app')?.__data?.data?.response?.contents?.twoColumnWatchNextResults)
    ?.secondaryResults?.secondaryResults?.results
    ?.map(r => r.compactAutoplayRenderer && Object.assign({}, r.compactAutoplayRenderer.contents[0].compactVideoRenderer, {autoPlay:true}) 
        || r.compactVideoRenderer &&  Object.assign({}, r.compactVideoRenderer, {autoPlay:false}) 
        || null)
    .filter(r => r != null)
    .map((v,i) =>({
        videoId:v.videoId, 
        title: v.title && v.title.simpleText || v.title && v.title.runs && v.title.runs[0].text, 
        thumb: v.thumbnail && v.thumbnail.thumbnails[v.thumbnail.thumbnails.length -1].url,
        channelTitle: v.longBylineText && v.longBylineText.runs[0].text, 
        publishAgo: v.publishedTimeText && v.publishedTimeText.simpleText, 
        viewText: v.viewCountText && v.viewCountText.simpleText || v.viewCountText && v.viewCountText.runs && v.viewCountText.runs[0].text,
        duration:v.lengthText && v.lengthText.simpleText, 
        channelId:v.channelId,
        rank:i+1
    }))
    ''')
                if all_recs is None:
                    all_recs = []
            except WebDriverException as e:
                findRecsEx = e
                break
            if(len(all_recs) >= 20):
                break
            rec_attempt = rec_attempt+1
            if(rec_attempt > 3):
                break
            await asyncio.sleep(2)

        if findRecsEx:
            unavalable = self.get_video_unavailable()
            if(unavalable == None):
                self.__log_driver_status('recommendations', findRecsEx.msg)
                raise findRecsEx

        rec_result = RecResult(all_recs, unavalable)
        self.log.debug('{tag} - about to store {recs} recs for {video}',
                       tag=self.user.tag, video=video_id, recs=len(all_recs))

        # store the information about the current video plus the corresponding recommendations
        self.get_video_features(video_id, rec_result)

        self.log.info('{tag} - recommendations collected for {video}', tag=self.user.tag, video=video_id)
        # return the recommendations
        return True

    def get_video_unavailable(self):
        reason = self.driver.execute_script('''
    var p = ytInitialPlayerResponse.playabilityStatus
    if(!p || p.status == 'OK') return null
    var reason = p.errorScreen?.playerErrorMessageRenderer?.reason?.simpleTex
        ?? p.errorScreen?.playerLegacyDesktopYpcOfferRenderer?.itemTitle
        ?? document.querySelector('#reason.yt-player-error-message-renderer')?.innerText
    var subReason = p.errorScreen?.subreason?.simpleText 
        ?? p.errorScreen?.subreason?.runs?.map(r => r.text).join('|') 
        ?? p.errorScreen?.playerLegacyDesktopYpcOfferRenderer?.offerDescription
        ?? document.querySelector('#subreason.yt-player-error-message-renderer')?.innerText
    return reason ? {reason, subReason} : null
       ''')
        return VideoUnavailable(reason['reason'], reason['subReason']) if reason else None

    SELECTOR_HISTORY = '#button[aria-label="Pause watch history"], #button[aria-label="Turn on watch history"]'

    def history_is_pause(self, e: WebElement):
        return e.get_attribute('aria-label') == 'Pause watch history'

    def history_pause(self):
        self.driver.get('https://www.youtube.com/feed/history')
        button = self.wait_for_clickable(Crawler.SELECTOR_HISTORY)
        if(self.history_is_pause(button)):
            button.click()
            self.wait_for_clickable('#button[aria-label="PAUSE"]').click()
        self.log.debug('{tag} - paused history', tag=self.user.tag)

    def history_resume(self):
        self.driver.get('https://www.youtube.com/feed/history')
        button = self.wait_for_clickable(Crawler.SELECTOR_HISTORY)
        if(not self.history_is_pause(button)):
            button.click()
            self.wait_for_clickable('#button[aria-label="TURN ON"]').click()
        self.log.debug('{tag} - resumed history', tag=self.user.tag)

    def delete_history(self):
        self.driver.get('https://www.youtube.com/feed/history')
        # self.__log_info('before_history_deletion')
        messages = self.driver.find_elements_by_xpath("//*[@id='message']")
        # if there are not videos in the history a text appears that says 'no videos here' but apparently there is a second, hidden, message with the same
        # id on the page. So instead of checking whether this element exists we differentiate between 1 message (there are videos in the history) and
        # two messages (there are no videos in the history)
        if len(messages) == 1:
            delete_buttons = self.wait.until(
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

    async def watch_videos(self, videos: List[str]):
        """
        This methods starts watching multiple videos in different tabs asynchronously, I.e. while one watch_video method is in the
        state of just sleeping, it already opens the next tab with another video.
        As soon as video is finished watching, the tab is closed

        Arguments:
            videos {list[str]} -- a list with all the video id's that are supposed to be watched
        """

        main_window = self.driver.window_handles[-1]
        for video_batch in chunked(videos, self.videos_parallel):
            tasks = []
            for video_id in video_batch:

                if self.store.exists(self.path.watch_time_json(video_id)):
                    self.log.info('{tag} - skipping watching video {video}', tag=self.user.tag, video=video_id)
                    continue

                handles_before = set(self.driver.window_handles)
                self.driver.execute_script("window.open('');")
                new_tab = (set(self.driver.window_handles)-handles_before).pop()
                tasks.append(self.watch_video(video_id, main_window, new_tab))
            await asyncio.gather(*tasks)

    async def watch_video(self, video_id: str, main_tab: str, current_tab: str):
        """[summary]
        starts video, skips any ads in the beginning and then watches video for an amount of time that is dependent on video length
        Currently we are ignoring ads that appear in the middle of the video. At the moment we are assuming that watching an ad also contributes
        to the watchtime of the video itself

        Arguments:
            video_id {str} -- id of video
            main_tab {str} -- the selenium window handle of the main tab to switch back to after current tab is closed
            current_tab {str} -- the tab in which the video shall be watched
        """

        self.driver.switch_to.window(current_tab)
        self.driver.get("https://www.youtube.com/watch?v=" + video_id)
        # wait until video is loaded

        try:
            # no need to click, it auto-plays
            play_button = self.wait_for_clickable('.ytp-play-button.ytp-button', error_expected=True)
            if play_button.get_attribute('aria-label') == 'Play (k)':
                play_button.click()
        except TimeoutException as e:
            unavailable = self.get_video_unavailable()
            if(unavailable == None):
                captcha = self.driver.find_elements_by_css_selector('form#captcha-form')
                if captcha:
                    self.__log_driver_status('form#captcha-form', 'we have been caught :(')
                    raise DetectedAsBotException
                else:
                    self.__log_driver_status('.ytp-play-button.ytp-button',
                                             'no play button or unavailable msg found. Probably a bug')
                raise e
            else:
                # the video didn't load, but it is unavailable for a reason. Log this as the result.
                watch_time = {
                    'account': self.user.tag,
                    'trial': self.trial_id,
                    'video_id': video_id,
                    'updated': datetime.utcnow().isoformat(),
                    'unavailable': asdict(unavailable) if unavailable else None,
                }
                self.store.save(self.path.watch_time_json(video_id), watch_time)
                return

        self.log.info('{tag} - started watching video {video}', tag=self.user.tag, video=video_id)

        # we store any advertisements that appear
        advertisements = dict(
            account=self.user.tag,
            trial=self.trial_id,
            video_id=video_id,
            updated=datetime.utcnow().isoformat(),
            advertisers=[]
        )

        def handle_ad() -> bool:
            """returns true when an add was found, false otherwise"""
            # unfortunately the ad loads slower than the player so we wait here to be sure we detect the ad if any appears
            time.sleep(2)  # blocking sleep (instead of await asyncio.sleep()) because we rely on the driver being on this tab
            ads = self.driver.find_elements_by_css_selector('button[id^=visit-advertiser] > span.ytp-ad-button-text')
            if len(ads) == 0:
                return False
            ad_text = ads[0].text
            self.log.info('{tag} - saw ad ({ad}) when watching {video}',
                          tag=self.user.tag, video=video_id, ad=ad_text)
            advertisements['advertisers'].append(ad_text)
            time.sleep(5)
            try:
                self.wait_for_clickable('*.ytp-ad-skip-button.ytp-button', error_expected=True).click()
                return True
            except TimeoutException:
                return False

        while handle_ad():
            time.sleep(1)
        # wait again, for second ad that might appear

        # measure for how long we are watching the actual video
        start_time = time.time()

        # upload the list of advertisers
        self.store.save(self.path.ad_json(video_id), advertisements)

        # detect the length of th actual video
        time_element = WebDriverWait(self.driver, 3).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'ytp-time-duration')))
        # we need to hover over the bar or else the time is not visible
        ActionChains(self.driver).move_to_element(time_element).perform()
        duration = time_element.text
        duration = self._get_seconds(duration)
        # to make sure that every video is watched long enough
        # watch_time = duration if duration < 300 else 300 if duration/3 < 300 else duration/3
        watch_time = duration if duration < self.max_watch_secs else self.max_watch_secs

        # let the asynchronous manager know that now other videos can be started
        await asyncio.sleep(watch_time)

        watch_time = {
            'account': self.user.tag,
            'trial': self.trial_id,
            'video_id': video_id,
            'video_length': duration,
            'goal_watch_time': watch_time,
            'watch_time': time.time()-start_time,
            'updated': datetime.utcnow().isoformat()
        }
        self.store.save(self.path.watch_time_json(video_id), watch_time)

        self.log.info('{tag} - finished watching {watch_time} of video {video}',
                      tag=self.user.tag, video=video_id, watch_time=watch_time['watch_time'])

        self.driver.switch_to.window(current_tab)
        self.driver.close()
        self.driver.switch_to.window(main_tab)

    def scan_feed(self, scan_num: int):
        # especially during the corona crisis, YouTube is offering a lot of extra information
        # they add noise to our data aquisition, because they influence how many videos are shown
        # on the home page, so we have to get rid of them
        # if we close these extra sections, YouTube remembers and doesnt show them again
        # ideally, this loop is only run once per account
        feed_path = self.path.feed_json(scan_num)
        if(self.store.exists(feed_path)):
            self.log.info('{tag} - skipping feed {scan_num}', tag=self.user.tag, scan_num=scan_num)
            return

        feed_is_bannerfree = False
        loop_break_index = 0
        while not feed_is_bannerfree:
            # refresh the feed everytime we had to close something until we finally get a completely clean feed

            if loop_break_index == 10:
                self.__log_driver_status('Feed Banner Detector is caught in loop - old feed')

            self.driver.get("https://www.youtube.com")

            if loop_break_index == 10:
                self.__log_driver_status('Feed Banner Detector is caught in loop - new feed')
                raise WebDriverException('Feed Banner Detector is caught in loop')
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
                themed_content = WebDriverWait(self.driver, 2).until(  # Kein Interesse
                    EC.presence_of_all_elements_located((By.XPATH, '//*[@aria-label="Not interested"]')))
            except TimeoutException:
                # print("No themed content")
                pass

            if themed_content is not None:
                feed_is_bannerfree = False
                for button in themed_content:
                    button.click()
                    print("banner closed")
            loop_break_index += 1

        all_videos = self.wait.until(
            EC.presence_of_all_elements_located((By.XPATH, '//*[@id="video-title-link"]'))
        )

        feed_info = dict(
            account=self.user.tag,
            trial=self.trial_id,
            feed_videos=[],
            updated=datetime.utcnow().isoformat()
        )
        for video in all_videos:
            # take the link and remove everything except for the id of the video that the link leads to
            vid_dict = dict(
                video_id=video.get_attribute('href').replace('https://www.youtube.com/watch?v=', ''),
                title=video.get_attribute('title'),
                full_info=video.get_attribute('aria-label')
            )
            feed_info['feed_videos'].append(vid_dict)

        self.store.save(feed_path, feed_info)
        self.log.info('{tag} - scanned feed {scan_num}', tag=self.user.tag, scan_num=scan_num)

    def __save_cookies(self):
        """saves all cookies
        """
        cookies = {'cookies': self.driver.get_cookies()}
        self.store.save(self.path.cookies_json(), cookies)

    def __load_cookies(self):
        """loads cookies for the current domain
        """
        cookiePath = self.path.cookies_json()
        cookies = self.store.load_dic(cookiePath)
        if(cookies == None):
            return
        currentUrl = urlparse(self.driver.current_url)
        for c in cookies['cookies']:
            if currentUrl.netloc.endswith(c['domain']):
                # not sure why, but this stops it being loaded.
                c.pop('expiry', None)
                try:
                    self.driver.add_cookie(c)
                except BaseException as e:
                    print(f'could not load cookies from: {cookiePath}: {e}')

    def __save_image(self, name: str):
        imagePath = self.path.session_path() / f'{name}.png'
        localImagePath = Path(tempfile.gettempdir()) / imagePath
        self.driver.get_screenshot_as_file(str(localImagePath))
        return localImagePath

    # easy method to save screenshots for headless mode
    def __log_driver_status(self, name: str, error: str = None):
        wd = self.driver

        seshPath = self.path.session_path()

        # save page source
        self.store.save(seshPath / f'{name}.html', wd.page_source)

        # save metadata
        state = {
            'name': name,
            'error': error,
            'url': wd.current_url,
            'title': wd.title,
            'updated': datetime.utcnow().isoformat()
        }
        self.store.save(seshPath / f'{name}.json', state)

        # save image
        local_image_path = self.__save_image(name)
        remote_image_path = seshPath / f'{name}.png'
        self.store.save_file(local_image_path, remote_image_path, content_type='image/png')
        image_url = self.store.url(remote_image_path)

        if(error != None):
            self.log.warn('{tag} - expereinced error ({error}) at {url} when ({phase}) {image_url}',
                          tag=self.user.tag, error=error, url=wd.current_url, phase=name, image_url=image_url)

        os.remove(local_image_path)

    def shutdown(self):
        self.driver.quit()
