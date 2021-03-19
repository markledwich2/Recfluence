
from typing import List, Optional
from dataclasses import dataclass, field
from dataclasses_jsonschema import JsonSchemaMixin, SchemaType
from dataclasses_jsonschema.type_defs import JsonSchemaMeta
from dotenv import load_dotenv
import os
import aiohttp


@dataclass
class SnowflakeCfg(JsonSchemaMixin):
    '''Snowflake configuration'''

    creds: str = field(
        metadata={"description": "email of the user e.g. mra.userscrape@gmail.com"})
    host: str = field(metadata={"description": ""})
    warehouse: str = field(metadata={"description": ""})
    db: str = field(metadata={"description": ""})
    schema: str = field(metadata={"description": ""})
    role: str = field(metadata={"description": ""})


@dataclass
class Cfg(JsonSchemaMixin):
    '''Recfluence configuration'''

    snowflake: SnowflakeCfg = field(metadata={
        "description": "cofniguration to connect to snowflake"})

    branchEnv: Optional[str] = field(metadata=JsonSchemaMeta(
        description="suffix to add to environment to avoid using production", required=False))


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

    env = os.getenv('env')
    if (env and env != 'prod' and cfg.branchEnv == None):
        cfg.branchEnv = env
    return cfg
