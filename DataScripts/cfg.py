
from typing import Optional
from dataclasses import dataclass, field
from dataclasses_jsonschema import JsonSchemaMixin
from dataclasses_jsonschema.type_defs import JsonSchemaMeta
from dotenv import load_dotenv
import os
import aiohttp


@dataclass
class StoreCfg(JsonSchemaMixin):
    dataStorageCs: str = field(metadata=JsonSchemaMeta(
        default="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=xxx;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
        description="connection string to the azure blob storage account storing the input, and output from scraping.",
        examples=["DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=xxx;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"],
        required=False
    ))
    container: str = field(default='userscrape', metadata=JsonSchemaMeta(description="the name of the container to store data"))


@dataclass
class SnowflakeCfg(JsonSchemaMixin):
    '''Snowflake configuration'''
    creds: str = field(metadata={"description": "email of the user e.g. mra.userscrape@gmail.com"})
    host: str
    warehouse: str
    db: str
    schema: str
    role: str


@dataclass
class SeqCfg(JsonSchemaMixin):
    '''Seq logging config'''
    seqUrl: str


@dataclass
class Cfg(JsonSchemaMixin):
    '''Subset of the recfluence cfg'''
    snowflake: SnowflakeCfg
    env: Optional[str] = field(metadata=JsonSchemaMeta(description="prod/dev", required=False))
    branchEnv: Optional[str] = field(metadata=JsonSchemaMeta(description="suffix to add to environment to avoid using production", required=False))
    storage: StoreCfg
    seq: SeqCfg


async def load_cfg() -> Cfg:
    '''loads application configuration form a blob from the cfg_sas environment variable
    '''
    load_dotenv()
    cfg_sas = os.getenv('cfg_sas')
    cfg: Cfg
    async with aiohttp.ClientSession() as sesh:
        async with sesh.get(cfg_sas) as r:
            json = await r.text()
            cfg = Cfg.from_json(json)

    cfg.env = os.getenv('env') or cfg.env
    cfg.branchEnv = os.getenv('branch_env') or cfg.branchEnv

    if(cfg.branchEnv != None):
        cfg.storage.container = f'{cfg.storage.container }-{cfg.branchEnv}'
        cfg.snowflake.db = f'{cfg.snowflake.db }_{cfg.branchEnv}'

    return cfg
