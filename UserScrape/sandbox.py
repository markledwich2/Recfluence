

from userscrape.cfg import load_cfg, UserCfg, Cfg
from userscrape.crawler import Crawler
import asyncio
from userscrape.store import BlobStore, new_trial_id
from azure.storage.blob import PublicAccess
import logging
from userscrape.log import configure_log
import os
import sys


async def setup_test_crawler() -> Crawler:
    cfg: Cfg = await load_cfg()
    store = BlobStore(cfg.store)
    store.ensure_container_exits(PublicAccess.Container)
    user = cfg.users[0]
    trial_id = new_trial_id()
    log = configure_log(cfg.seqUrl, os.getenv('env'), cfg.branch_env, trial_id)
    crawler = Crawler(store, None, user, cfg.headless, trial_id, log)
    return crawler


async def test_log():
    cfg: Cfg = await load_cfg()
    log = configure_log(cfg.seqUrl, os.getenv('env'), cfg.branch_env, 'logest')
    log.debug("debug 1")
    await asyncio.sleep(2)
    log.debug("debug 2")
    try:
        raise EnvironmentError()
    except EnvironmentError as ex:
        log.error("unhandled environment error", exc_info=True)

    log.debug("debug 3")
    logging.shutdown()
    sys.exit(1)


async def test_watch(video_id: str):
    crawler: Crawler = await setup_test_crawler()
    await crawler.watch_videos([video_id])

# asyncio.run(test_watch('hYx2t-iEZu0'))
asyncio.run(test_log())
