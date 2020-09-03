
from typing import List, Optional
from dataclasses import dataclass, field
from dataclasses_jsonschema import JsonSchemaMixin, SchemaType
from dataclasses_jsonschema.type_defs import JsonSchemaMeta
from dotenv import load_dotenv
import os
import aiohttp


@dataclass
class UserCfg(JsonSchemaMixin):
    email: str = field(
        metadata={"description": "email of the user e.g. mra.userscrape@gmail.com"})
    password: str = field(metadata={"description": "password for the user "})
    tag: str = field(metadata=JsonSchemaMeta({
        "description": "The users tag, expected to be unique between users",
        "examples": [
            "Partisan Right",
            "White Identitarian",
            "Provocative Anti-SJW",
            "Anti-theist",
            "Religious Conservative",
            "Partisan Left", "MRA",
            "Anti-SJW",
            "Socialist",
            "Center/Left MSM",
            "Libertarian",
            "Conspiracy",
            "Social Justice"
        ]}))
    telephone_number: Optional[str] = field(default=None, metadata=JsonSchemaMeta(
        description="telephone number to verify account ", required=False))
    notify_discord_user_id: Optional[int] = field(default=None, metadata=JsonSchemaMeta(
        description="the user id (e.g. 123465448467005488) in discord to notify", required=False))


@dataclass
class DiscordCfg(JsonSchemaMixin):
    bot_token: str = field(metadata={"description": "The auth token for the discord bot"})
    channel_id: int = field(metadata={"description": "The channel to ask for user validation codes"})


@dataclass
class StoreCfg(JsonSchemaMixin):

    cs: str = field(metadata=JsonSchemaMeta(
        default="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
        description="connection string to the azure blob storage account storing the input, and output from scraping.",
        examples=["DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"],
        required=False
    ))

    container: str = field(default='userscrape', metadata=JsonSchemaMeta(
        description="the name of the container to store data", required=False))

    root_path: str = field(default='', metadata=JsonSchemaMeta(
        description="the root folder path to store data (e.g. folder1/folder2", required=False))


@dataclass
class Cfg(JsonSchemaMixin):
    '''UserScrape configuration'''

    users: List[UserCfg] = field(metadata={
                                 "description": "the YouTube viewing users in the experiment. Contains credentials and other information"})

    headless: bool = field(metadata={
                           "description": "When true, selenium will run without an interactive browser showing. Must be true when running in a container"})

    discord: DiscordCfg = field(metadata={
        "description": "configuration for the discord bot used to request user validation"})

    seqUrl: str = field(metadata=JsonSchemaMeta({
        "description": "url of your seq instance",
        "examples": ["http://log.recfluence.net/", "http://localhost:5341/"]}))

    store: StoreCfg = field(metadata=JsonSchemaMeta(description="storage configuration"))

    feed_scans: int = field(default=20, metadata=JsonSchemaMeta(
        description="number of times to collect the list of videos in the feed", required=False))
    videos_parallel: int = field(default=5, metadata=JsonSchemaMeta(
        description="number of video's to watch in parallel", required=False))

    init_seed_vids: int = field(default=50, metadata=JsonSchemaMeta(
        description="the number of videos to watch when initializing", required=False))
    run_seed_vids: int = field(default=5, metadata=JsonSchemaMeta(
        description="the number of videos to watch when performing a daily run", required=False))
    run_test_vids: Optional[int] = field(default=None, metadata=JsonSchemaMeta(
        description="the number recommendations to collect. Only define if you want to restrict for test purposes", required=False))

    branch_env: Optional[str] = field(default=None, metadata=JsonSchemaMeta(
        description="a name to prefix/suffix names of environment objects to have clean branch environments", required=False))

    max_watch_secs: int = field(default=300, metadata=JsonSchemaMeta(
        description="the maximum time to watch a seed video for", required=False))

    browser: str = field(default="firefox", metadata=JsonSchemaMeta(
        description="The browser to use for scraping", examples=["chrome", "firefox"],
        required=False))


async def load_cfg() -> Cfg:
    '''loads application configuration form a blob (if defined in .env cfg_sas) or ./userscrape.json
    '''
    load_dotenv()
    cfg_sas = os.getenv('cfg_sas')

    cfg: Cfg
    if (cfg_sas):
        async with aiohttp.ClientSession() as sesh:
            async with sesh.get(cfg_sas) as r:
                cfg = Cfg.from_json(await r.text())
    else:
        with open('userscrape.json', "r") as r:
            cfg = Cfg.from_json(r.read())

    env = os.getenv('env')
    if (env and env != 'prod' and cfg.branch_env == None):
        cfg.branch_env = env

    if(cfg.branch_env is not None):
        cfg.store.container = f'{cfg.store.container}-{cfg.branch_env}'

    return cfg
