import sys
import os
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options  # for headless mode
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.remote.webelement import WebElement
from subprocess import Popen,PIPE
import asyncio

# proxy_key = os.environ.get('proxy_key')
# if proxy_key == None: raise EnvironmentError("proxy_key required")

# use_proxy = True if os.environ.get('use_proxy') == '1' else False

creds = os.environ.get('userscrape_creds')
if creds == None: raise EnvironmentError("'userscrape_creds' env varaible required in the format 'email:pass'")
username, password = creds.split(':')

# #sys.path.insert(0,'/usr/lib/chromium-browser/chromedriver')
# #crawlera_out = Popen(f'/usr/bin/crawlera-headless-proxy -a {proxy_key}')
# if(use_proxy):
#     try:
#         proc = Popen(['crawlera-headless-proxy', '-a', proxy_key])
#     except:
#         print("could not start proxy service")
#         use_proxy = False

#output = proc.communicate()[0]

options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36")
# if(use_proxy): options.add_argument('--proxy-server=localhost:3128') # use the crawlera headless proxy (ca't get this to run in colab)
capabilities = DesiredCapabilities.CHROME.copy()
capabilities['acceptSslCerts'] = True
capabilities['acceptInsecureCerts'] = True

# disable images to speed up the page loading
options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})

wd = webdriver.Chrome(options=options, desired_capabilities=capabilities)
wd.get('https://httpbin.org/ip')
pre:WebElement = wd.find_element_by_css_selector('pre')

print(f'{"proxy" if use_proxy else "direct"} IP {json.loads(pre.text)["origin"]}')

from crawler import Crawler
blob_sas = os.getenv('yt_scraper_sas') or 'https://pyt.blob.core.windows.net/testdata?st=2020-04-04T05%3A16%3A12Z&se=2087-04-05T05%3A16%3A00Z&sp=racwdl&sv=2018-03-28&sr=c&sig=3P9C41eTPviKxgWXnRK%2FD%2Fj4ZCCFtf2N%2BxEVGBkJVFM%3D'
crawler = Crawler(wd, blob_sas)

crawler.login(username, password)

