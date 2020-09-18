import os
from selenium.common.exceptions import WebDriverException
from userscrape.cfg import load_cfg, UserCfg, Cfg
from userscrape.crawler import Crawler, DetectedAsBotException
from userscrape.data import UserScrapeData
from userscrape.store import BlobStore, new_trial_id
from userscrape.discord_bot import DiscordBot
from userscrape.log import configure_log
from userscrape.results import load_incomplete_trial, save_complete_trial, TrialCfg, save_incomplete_trial
from userscrape.format import format_seconds
import asyncio
import argparse
from typing import List, Any
from azure.storage.blob import PublicAccess
import logging
import time
import sys
import random


async def experiment(initialization: bool, accounts: List[str], trial_id=None):
    cfg: Cfg = await load_cfg()
    env = os.getenv('env') or 'dev'
    store = BlobStore(cfg.store)

    load_trial = trial_id is not None
    trial_id = trial_id if trial_id else new_trial_id()
    log = configure_log(cfg.seqUrl, trial_id=trial_id, env=env, branch_env=cfg.branch_env)

    trial_cfg = None
    if load_trial:
        trial_cfg = load_incomplete_trial(trial_id, store, log)
        if accounts:
            accounts = list(set(accounts).intersection(trial_cfg.accounts)) if trial_cfg else accounts
        else:
            accounts = trial_cfg.accounts if trial_cfg else None

    if not load_trial or trial_cfg is None:
        save_incomplete_trial(TrialCfg(trial_id, accounts), store, log)

    # get users from trial if exists.

    users: List[UserCfg] = [u for u in cfg.users if accounts == None or u.tag in accounts]
    random.shuffle(users)
    tags = [user.tag for user in users]

    log.info("Trail {trial_id} started - Init={initialization}, Accounts={accounts}",
             trial_id=trial_id, initialization=initialization, accounts='|'.join(tags))

    trial_start_time = time.time()

    store.ensure_container_exits(PublicAccess.Container)
    data = UserScrapeData(store, trial_id)

    videos_to_test = data.test_videos(cfg.run_test_vids)
    videos_to_seed = data.seed_videos(cfg.init_seed_vids if initialization else cfg.run_seed_vids)

    bot = DiscordBot(cfg.discord)
    await bot.start_in_backround()

    failedTags = set()
    detected_as_bot = False

    try:
        for user in users:
            attempts = 0
            success = False
            start = time.time()

            while not success and attempts < 3 and not detected_as_bot:
                attempts = attempts + 1
                log.info('{tag} - started scraping', tag=user.tag)

                crawler = Crawler(store, bot, user, cfg, trial_id, log)
                user_seed_videos = videos_to_seed[user.tag] if user.tag in videos_to_seed else []
                user_seed_video_ids: List[str] = [video.video_id for video in user_seed_videos]

                try:
                    await crawler.load_home_and_login()
                    log.info("{tag} - logged in", tag=user.tag)

                    log.info("{tag} - seeding with {videos}", tag=user.tag, videos='|'.join(user_seed_video_ids))
                    crawler.history_resume()
                    await crawler.watch_videos(user_seed_video_ids)
                    for i in range(cfg.feed_scans):
                        crawler.scan_feed(i)

                    log.info("{tag} - collecting recommendations for {video_number} videos",
                             tag=user.tag, video_number=len(videos_to_test))
                    crawler.history_pause()
                    rec_failures = 0
                    for video in videos_to_test:
                        try:
                            await crawler.get_recommendations_for_video(video.video_id)
                        except WebDriverException as e:
                            rec_failures = rec_failures + 1
                            log.warning('{tag} - scraping error collecting recommendations for video {video_id} (failed this trial: {rec_failures}): {error}',
                                        tag=user.tag, video_id=video.video_id, rec_failures=rec_failures, error=e)
                    success = True
                    log.info("{tag} - complete in {duration}", tag=user.tag,
                             duration=format_seconds(time.time() - start))

                except DetectedAsBotException:
                    detected_as_bot = True
                    log.warning('{tag} - detected as bot in trial {trial_id}. Aborting for all users.',
                                tag=user.tag, trial_id=trial_id, exc_info=1)
                except BaseException:
                    log.warning('{tag} - unhandled error in trial {trial_id} (attempts {attempts})',
                                tag=user.tag, trial_id=trial_id, attempts=attempts, exc_info=1)
                finally:
                    crawler.shutdown()

                if not success:
                    failedTags.add(user.tag)
    finally:
        await bot.close()

    if(len(failedTags) == 0):
        log.info("completed trial {trial_id} in {duration} for {accounts}", trial_id=trial_id,
                 duration=format_seconds(time.time() - trial_start_time), accounts='|'.join(tags))
        save_complete_trial(trial_id, store, log)
        logging.shutdown()
    else:
        log.error('trail ({trial_id}) failed for users {users}. Investigate and re-run this trail (it will resume incomplete tasks)',
                  users='|'.join(failedTags), trial_id=trial_id)
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
     Examples: \n \
        WhiteIdentitarian \n \
        MRA \n \
        Conspiracy \n \
        Libertarian \n \
        Fresh (a fresh account without viewing history)",
                        default=None)
    args = parser.parse_args()

    asyncio.run(experiment(
        args.init,
        args.accounts.split('|') if args.accounts else None,
        args.trial.strip() if args.trial and len(args.trial) > 0 else None)
    )
