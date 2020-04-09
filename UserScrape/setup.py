from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from typing import List
import json
from dataclasses import dataclass
from dataclasses_json import dataclass_json

@dataclass_json
@dataclass
class UserCfg:
    email: str
    password: str

@dataclass_json
@dataclass
class Cfg:
    storage_sas:str
    users: List[UserCfg]


def app_cfg(cfg_sas:str) -> Cfg:
    '''loads the application configuration from appcfg.json in the given azure blob container
    
    Arguments:
        cfg_sas {str} -- a SAS uri with read access
    
    Returns:
        Cfg -- the application configuration
    '''
    container = ContainerClient.from_container_url(cfg_sas)
    json = container.download_blob('appcfg.json').content_as_text()
    cfg:Cfg = Cfg.from_json(json)
    return cfg