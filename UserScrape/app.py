import os
from cfg import load_cfg
from crawler import Crawler
from selenium.common.exceptions import NoSuchElementException
from data import load_all_seeds, seeds_for_user

cfg = load_cfg()
video_seeds_df = load_all_seeds()

for user in cfg.users:
    print(f'scraping for user {user.email}')
    crawler = Crawler(cfg.data_storage_cs, user.email, user.password, cfg.headless)
    crawler.test_ip()

    user_seed_videos = seeds_for_user(user, video_seeds_df)

    try:
        crawler.load_home_and_login()
        # crawler.login()
        # for video in user_seed_videos:
        #     crawler.get_recommendations_for_video(video.video_id)
        crawler.watch_video('Pn9TWf3wNaQ')

        crawler.shutdown()
    except NoSuchElementException as e:
        print(f'Not able to find a required element {e.msg}. {user.email}')
    finally:
        crawler.shutdown()
