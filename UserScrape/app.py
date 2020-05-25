import os
from cfg import load_cfg
from crawler import Crawler
from selenium.common.exceptions import NoSuchElementException
from data import load_all_seeds, seeds_for_user
import asyncio
import random
from store import BlobStore
from azure.storage.blob import PublicAccess
from discord_bot import DiscordBot

async def main():
    cfg = load_cfg()
    video_seeds_df = load_all_seeds()
    repetitions = 2  # 100
    # todo: this list of videos needs to be sampled
    test_videos = ['uo9dAIQR3g8', 'CH50zuS8DD0', '9_R3_CThc38']

    store = BlobStore(cfg.data_storage_cs, 'userscrape')
    store.ensure_exits(PublicAccess.Container)

    bot = DiscordBot(cfg.discord)
    await bot.start_in_backround()

    try:
        for user in cfg.users:

            print(f'scraping for user {user.email}')
            crawler = Crawler(store, bot, user, cfg.headless)
            #crawler.test_ip()

            user_seed_videos = seeds_for_user(user, video_seeds_df)

            try:
                await crawler.load_home_and_login()
                #await crawler.login()
                crawler.delete_history()
                for repetition in range(1, repetitions):
                    await crawler.watch_videos([video.video_id for video in user_seed_videos[0:5]])
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

asyncio.run(main())