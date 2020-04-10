import os
from dotenv import load_dotenv
from setup import app_cfg
from crawler import Crawler
from selenium.common.exceptions import NoSuchElementException

load_dotenv()
cfg_sas = os.getenv('cfg_sas')
cfg = app_cfg(cfg_sas)
headless = os.getenv('headless') == '1' or os.getenv('headless') == 'true'

for user in cfg.users:
    
    print(f'scraping for user {user.email}')
    crawler = Crawler(cfg.storage_sas, user.email, user.password, headless)
    try:
        crawler.load_home_and_login()
        # crawler.watch_video('UnSILVWDKL8')
        # crawler.get_recommendations_for_video('dQYf-AI5m0Q')
        crawler.shutdown()
    except NoSuchElementException as e:
        print(f'Not able to find a required element {e.msg}. {user.email}')
    finally:
        crawler.shutdown()
