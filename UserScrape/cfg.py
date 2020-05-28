from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from typing import List, Optional
import json
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from dataclasses_jsonschema import JsonSchemaMixin, SchemaType
from dataclasses_jsonschema.type_defs import JsonSchemaMeta


@dataclass_json
@dataclass
class UserCfg(JsonSchemaMixin):
    email: str = field(
        metadata={"description": "email of the user e.g. mra.userscrape@gmail.com"})
    password: str = field(metadata={"description": "password for the user "})
    telephone_number: str = field(metadata={"description": "telephone number to verify account "})
    ideology: str = field(metadata=JsonSchemaMeta({
        "description": "The users ideology, expected to be unique between users",
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
    notify_discord_user_id: Optional[int] = field(metadata=JsonSchemaMeta(
        {"description": "the user id (e.g. 123465448467005488) in discord to notify", "required": False}))


@dataclass_json
@dataclass
class DiscordCfg(JsonSchemaMixin):
    bot_token: str = field(metadata={"description": "The auth token for the discord bot"})
    channel_id: int = field(metadata={"description": "The channel to ask for user validation codes"})


@dataclass_json
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


@dataclass_json
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


def load_cfg() -> Cfg:
    '''loads application configuration from userscrape.json
    '''
    with open('userscrape.json', "r") as r:
        cfg = Cfg.from_json(r.read())
    return cfg
