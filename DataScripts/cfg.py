
from typing import List, Optional
from dataclasses import dataclass
from dataclasses_json.api import DataClassJsonMixin
from dotenv import load_dotenv
import os
import aiohttp


@dataclass
class StoreCfg:
    dataStorageCs: str
    container: str


@dataclass
class SnowflakeCfg:
    creds: str
    host: str
    warehouse: str
    db: str
    schema: str
    role: str


@dataclass
class SeqCfg:
    seqUrl: str


@dataclass
class RunState(DataClassJsonMixin):
    videoPaths: Optional[List[str]] = None


@dataclass
class Cfg(DataClassJsonMixin):
    snowflake: SnowflakeCfg
    storage: StoreCfg
    seq: SeqCfg
    state: RunState = None
    env: Optional[str] = None
    branchEnv: Optional[str] = None


async def load_cfg() -> Cfg:
    '''loads application configuration form a blob from the cfg_sas environment variable'''
    load_dotenv()
    cfg_sas = os.getenv('cfg_sas')
    cfg: Cfg
    async with aiohttp.ClientSession() as sesh:
        async with sesh.get(cfg_sas) as r:
            json = await r.text()
            cfg = Cfg.from_json(json)

    cfg.env = os.getenv('env') or cfg.env
    cfg.branchEnv = os.getenv('branch_env') or cfg.branchEnv

    runStateJson = os.getenv('run_state')
    cfg.state = RunState.from_json(runStateJson) if runStateJson else RunState()

    if(cfg.branchEnv != None):
        cfg.storage.container = f'{cfg.storage.container }-{cfg.branchEnv}'
        cfg.snowflake.db = f'{cfg.snowflake.db }_{cfg.branchEnv}'

    return cfg
