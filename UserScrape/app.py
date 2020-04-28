import os
from cfg import load_cfg
from crawler import Crawler
from selenium.common.exceptions import NoSuchElementException

cfg = load_cfg()

for user in cfg.users:
    print(f'scraping for user {user.email}')
    crawler = Crawler(cfg.data_storage_cs, user.email, user.password, cfg.headless)
    crawler.test_ip()
    try:
        crawler.load_home_and_login()
        crawler.shutdown()
    except NoSuchElementException as e:
        print(f'Not able to find a required element {e.msg}. {user.email}')
    finally:
        crawler.shutdown()
