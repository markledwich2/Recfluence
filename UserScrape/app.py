import os
from userscrape.cfg import load_cfg, UserCfg, Cfg
from userscrape.crawler import Crawler, DetectedAsBotException
from userscrape.data import UserScrapeData
from userscrape.store import BlobStore, new_trial_id
from userscrape.discord_bot import DiscordBot
from userscrape.log import configure_log
from userscrape.results import save_complete_trial
from userscrape.format import format_seconds
from selenium.common.exceptions import NoSuchElementException, WebDriverException
import asyncio
import argparse
from typing import List
from azure.storage.blob import PublicAccess
import logging
import time
import seqlog
import shortuuid
import sys
from seqlog import log_to_seq


async def experiment(initialization: bool, accounts: List[str], trial_id=None):
    env = os.getenv('env') or 'dev'
    cfg: Cfg = await load_cfg()
    trial_id = trial_id if trial_id else new_trial_id()
    log = configure_log(cfg.seqUrl, trial_id=trial_id, env=env)
    users: List[UserCfg] = [u for u in cfg.users if accounts == None or u.ideology in accounts]

    log.info("Trail {trial_id} started - Init={initialization}, Accounts={accounts}",
             trial_id=trial_id, initialization=initialization, accounts='|'.join([user.email for user in users]))

    trial_start_time = time.time()

    store = BlobStore(cfg.store)
    store.ensure_container_exits(PublicAccess.Container)
    data = UserScrapeData(store, trial_id)

    videos_to_test = data.test_videos(cfg.run_test_vids)
    videos_to_seed = data.seed_videos(cfg.init_seed_vids if initialization else cfg.run_seed_vids)

    bot = DiscordBot(cfg.discord)
    await bot.start_in_backround()

    failedUsers = []
    detected_as_bot = False

    try:
        for user in users:
            if(detected_as_bot):
                failedUsers.append(user.email)
                continue

            log.info('{email} - started scraping', email=user.email)
            start = time.time()

            crawler = Crawler(store, bot, user, cfg.headless, trial_id, log)
            user_seed_videos = videos_to_seed[user.ideology] if user.ideology in videos_to_seed else []
            user_seed_video_ids: List[str] = [video.video_id for video in user_seed_videos]

            try:
                await crawler.load_home_and_login()
                log.info("{email} - logged in", email=user.email)
                crawler.delete_history()

                log.info("{email} - seeding with {videos}", email=user.email, videos='|'.join(user_seed_video_ids))
                await crawler.watch_videos(user_seed_video_ids)
                for i in range(cfg.feed_scans):
                    crawler.scan_feed(i)

                log.info("{email} - collecting recommendations for {video_number} videos",
                         email=user.email, video_number=len(videos_to_test))
                for video in videos_to_test:
                    if await crawler.get_recommendations_for_video(video.video_id):
                        await crawler.delete_last_video_from_history(video.video_id)
                # crawler.delete_history()
                log.info("{email} - complete in {duration}", email=user.email,
                         duration=format_seconds(time.time() - start))
            except DetectedAsBotException:
                log.error('{email} - detected as bot. aborting scraping for all users', email=user.email, exc_info=1)
                failedUsers.append(user.email)
                detected_as_bot = True
            except BaseException:
                log.error('{email} - unhandled error. aborting scraping for this user', email=user.email, exc_info=1)
                failedUsers.append(user.email)
            finally:
                crawler.shutdown()
    finally:
        await bot.close()

    if(len(failedUsers) == 0):
        log.info("completed trial {trial_id} in {duration}", trial_id=trial_id,
                 duration=format_seconds(time.time() - trial_start_time))
        save_complete_trial(trial_id, store, log)
        logging.shutdown()
    else:
        log.error('trail ({trial_id}) failed for users {users}. Investigate and re-run this trail (it will resume incomplete tasks)',
                  users='|'.join(failedUsers), trial_id=trial_id)
        logging.shutdown()
        sys.exit(13 if detected_as_bot else 1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start one iteration of the experiment. If you run this for the first time \
        set --init. If you do not want to run the experiment with all accounts, specify the list with --accounts')
    parser.add_argument("--init", "-i",
                        help="Provide this argument if the experiment shall start from the beginning. Each account will start with a clear history and will watch 50 initial videos",
                        action='store_true',
                        default=False)

    parser.add_argument("--trial", "-t",
                        help="provide a trial ID if you want to continue an existing trial that is not complete",
                        default=None,
                        nargs='?', const='')

    parser.add_argument("--accounts", "-a",
                        help="A | separeted list users for which the corresponding accounts will be included in this run of the experiment. \
     Possible options are \n \
        White Identitarian \n \
        MRA \n \
        Conspiracy \n \
        Libertarian \n \
        Provocative Anti-SJW \n \
        Anti-SJW \n \
        Socialist \n \
        Religious Conservative \n \
        Social Justice \n \
        Center/Left MSM \n \
        Partisan Left \n \
        Partisan Right \n \
        Anti-Theist \n \
        Uniform (An equal number of videos from each class) \n \
        Proportional (Videos are watched proportional to their Viewcount) \n \
        Non-Political (Non-political videos) \n \
        Fresh (a fresh account without viewing history)",
                        default=None)
    args = parser.parse_args()

    asyncio.run(experiment(
        args.init,
        args.accounts.split('|') if args.accounts else None,
        args.trial if args.trial and len(args.trial) > 0 else None)
    )
