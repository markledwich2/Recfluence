import os
from cfg import load_cfg
from crawler import Crawler
from selenium.common.exceptions import NoSuchElementException
from data import load_all_seeds, seeds_for_user
import asyncio
import argparse
from typing import List
from store import BlobStore
from azure.storage.blob import PublicAccess
from discord_bot import DiscordBot

async def experiment(initialization: bool, accounts: List[int]):
    cfg = load_cfg()
    video_seeds_df = load_all_seeds()

    cfg = load_cfg()
    if initialization:
        video_seeds_df = load_all_seeds()
    else:
        # todo: function that returns only 5 sampled videos
        video_seeds_df = load_all_seeds()[0:5]

    repetitions = 2 # 100
    # todo: this list of videos needs to be sampled
    test_videos = ['uo9dAIQR3g8', 'CH50zuS8DD0', '9_R3_CThc38']

    
    store = BlobStore(cfg.data_storage_cs, 'userscrape')
    store.ensure_exits(PublicAccess.Container)

    bot = DiscordBot(cfg.discord)
    await bot.start_in_backround()

    try:
        for user in [cfg.users[i] for i in accounts]:
            print(f'scraping for user {user.email}')
            crawler = Crawler(store, bot, user, cfg.headless)
            #crawler.test_ip()

            user_seed_videos = seeds_for_user(user, video_seeds_df)

            try:
                # crawler.load_home_and_login()
                crawler.login()
                crawler.delete_history()
                for repetition in range(1,repetitions):
                    asyncio.run(crawler.watch_videos([video.video_id for video in user_seed_videos[0:5]]))
                    crawler.scan_feed()
                    # 115 test videos
                    for video in test_videos:
                        crawler.get_recommendations_for_video(video)
                        crawler.delete_last_video_from_history(video)
                    crawler.delete_history()
                    crawler.update_trial()
                    crawler.shutdown()
            except NoSuchElementException as e:
                print(f'Not able to find a required element {e.msg}. {user.email}')
            finally:
                crawler.shutdown()
    finally:
        await bot.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start one iteration of the experiment. If you run this for the first time \
        set --init. If you do not want to run the experiment with all accounts, specifiy the list with --accounts')
    parser.add_argument("--init", "-i", 
    help="Provide this argument if the experiment shall start from the beginning. Each account will start with a clear history and will watch 50 initial videos",
    action='store_true')
    parser.add_argument("--accounts", "-a", 
    help="A list of numbers for which the corresponding accounts will be included in this run of the experiment. \
     Possible options are \n \
        0. White Identitarian \n \
        1. MRA \n \
        2. Conspiracy \n \
        3. Libertarian \n \
        4. Provocative Anti-SJW \n \
        5. Anti-SJW \n \
        6. Socialist \n \
        7. Religious Conservative \n \
        8. Social Justice \n \
        9. Center/Left MSM \n \
        10. Partisan Left \n \
        11. Partisan Right \n \
        12. Anti-Theist \n \
        13. Uniform (An equal number of videos from each class) \n \
        14. Videos are watched proportional to their Viewcount \n \
        15. Non-Political (Non-political videos) \n \
        16. A fresh account without viewing history ",
        default=list(range(0,17)),
        nargs="+", type=int,
        choices=list(range(0,17))
        )
    args = parser.parse_args()

    asyncio.run(experiment(args.init, args.accounts))
