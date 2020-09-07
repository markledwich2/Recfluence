

from typing import Any, Iterable, Iterator, List
from pathlib import Path, PurePath, PurePosixPath
import tempfile
import json
import os
import shortuuid
from logging import Logger
from .store import BlobPaths, BlobStore, file_date_str
from dataclasses import dataclass
from dataclasses_jsonschema import JsonSchemaMixin


@dataclass
class TrialCfg(JsonSchemaMixin):
    trial_id: str
    accounts: List[str]


def load_incomplete_trial(trial_id: str, store: BlobStore, log: Logger) -> TrialCfg:
    paths = BlobPaths(store.cfg, trial_id)
    p = paths.trial_cfg_json()
    if not store.exists(p):
        return None
    return TrialCfg.from_json(store.load(p))


def save_incomplete_trial(trial_cfg: TrialCfg, store: BlobStore, log: Logger):
    path = BlobPaths(store.cfg, trial_cfg.trial_id)
    store.save(path.trial_cfg_json(), trial_cfg.to_json())


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

    path = BlobPaths(store.cfg, trial_id)
    res_path = path.results_path_out()
    save_complete_jsons(path.rec_path(), res_path / 'rec')
    save_complete_jsons(path.feed_path(), res_path / 'feed')
    save_complete_jsons(path.ad_path(), res_path / 'ad')
    save_complete_jsons(path.watch_time_path(), res_path / 'watch')
