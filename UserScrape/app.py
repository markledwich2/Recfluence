import os
from cfg import load_cfg
from crawler import Crawler
from selenium.common.exceptions import NoSuchElementException
from data import load_all_seeds, seeds_for_user
import asyncio
import argparse

def experiment(initialization: bool, accounts: list[int]):
    cfg = load_cfg()
    video_seeds_df = load_all_seeds()

    for user in cfg.users:
        print(f'scraping for user {user.email}')
        crawler = Crawler(cfg.data_storage_cs, user.email, user.password, user.telephone_number, cfg.headless)
        crawler.test_ip()

        user_seed_videos = seeds_for_user(user, video_seeds_df)

        try:
            crawler.load_home_and_login()
            # crawler.login()
            # for video in user_seed_videos:
            #     crawler.get_recommendations_for_video(video.video_id)
            
            # video with ads for testing: 'Pn9TWf3wNaQ'
            asyncio.run(crawler.watch_videos(['uo9dAIQR3g8', 'CH50zuS8DD0', '9_R3_CThc38']))

            crawler.shutdown()
        except NoSuchElementException as e:
            print(f'Not able to find a required element {e.msg}. {user.email}')
        finally:
            crawler.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--init", "-i", 
    help="Provide this argument if the experiment shall start from the beginning. Each account will start with a clear history and will watch 50 initial videos",
    default=False)
    parser.add_argument("--accounts", "-a", 
    help="A list of numbers for which the corresponding accounts will be included in this run of the experiment. \
     Possible options are \n \
        1. White Identitarian \n \
        2. MRA \n \
        3. Conspiracy \n \
        4. Libertarian \n \
        5. Provocative Anti-SJW \n \
        6. Anti-SJW \n \
        7. Socialist \n \
        8. Religious Conservative \n \
        9. Social Justice \n \
        10. Center/Left MSM \n \
        11. Partisan Left \n \
        12. Partisan Right \n \
        13. Anti-Theist \n \
        14. Uniform (An equal number of videos from each class) \n \
        15. Videos are watched proportional to their Viewcount \n \
        16. Non-Political (Non-political videos) \n \
        17. A fresh account without viewing history ",
        default=list(range(1,18)))
    args = parser.parse_args()
    initialization = False
    if args.init:
        initialization = True
    experiment(initialization, args.accounts)