import sys
import os
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options  # for headless mode
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.remote.webelement import WebElement
from subprocess import Popen,PIPE
from dotenv import load_dotenv

import asyncio

load_dotenv()
use_proxy = True if os.environ.get('use_proxy') == '1' else False

creds = os.environ.get('youtube_creds')
if creds == None:
    print("No 'youtube_creds' env varaible provided . Enter your credentials")
    creds = input("Enter your google creds (format 'email:pass'): ")
email, password = creds.split(':')

options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
# this is mark@ledwich.com's recently used user agent.
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36")
# if(use_proxy): options.add_argument('--proxy-server=localhost:3128') # use the crawlera headless proxy (ca't get this to run in colab)
capabilities = DesiredCapabilities.CHROME.copy()
capabilities['acceptSslCerts'] = True
capabilities['acceptInsecureCerts'] = True

# disable images to speed up the page loading
options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})

wd = webdriver.Chrome(options=options, desired_capabilities=capabilities)
#wd.get('https://httpbin.org/ip')
#pre:WebElement = wd.find_element_by_css_selector('pre')
#print(f'{"proxy" if use_proxy else "direct"} IP {json.loads(pre.text)["origin"]}')

from crawler import Crawler
storage_sas = os.getenv('storage_sas')
if(storage_sas == None): raise EnvironmentError("")

crawler = Crawler(wd, storage_sas, email, password)
crawler.load_home_and_login()
