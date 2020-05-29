
from store import UserScrapePaths, BlobStore, file_date_str
from azure.storage.blob import PublicAccess, BlobProperties
from azure.core.paging import ItemPaged
from typing import Iterator, Iterable, Dict, Any
from pathlib import Path, PurePath, PurePosixPath
import tempfile
import json
import os
import shortuuid
from cfg import load_cfg
from logging import Logger


def save_complete_trial(trial_id: str, store: BlobStore, log: Logger):

    def save_complete_jsons(source: PurePath, dest: PurePath):
        source_Files = store.list(source)
        file_name = f'{file_date_str()}.{shortuuid.random(4)}.jsonl'
        localPath = Path(tempfile.gettempdir(), 'UserScrape', file_name)
        localPath.parent.mkdir(parents=True, exist_ok=True)
        dest_file = dest / file_name
        with open(localPath, "w", encoding="utf-8") as w:
            for file in source_Files:
                d = store.load_dic(PurePath(file.name))
                w.write(json.dumps(d, indent=None) + '\n')
        store.save_file(localPath, dest_file)
        os.remove(localPath)
        log.info('saved completed trial {trial_id} results to {dest_file}',
                 trial_id=trial_id, dest_file=dest_file.as_posix())

    path = UserScrapePaths(store.cfg, trial_id)
    res_path = path.results_path()
    save_complete_jsons(path.rec_path(), res_path / 'rec')
    save_complete_jsons(path.feed_path(), res_path / 'feed')
    save_complete_jsons(path.ad_path(), res_path / 'ad')
    save_complete_jsons(path.watch_time_path(), res_path / 'watch')
