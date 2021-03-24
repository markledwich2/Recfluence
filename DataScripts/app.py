
from args import Args, load_args
from datetime import datetime, timezone
import gzip
from itertools import chain
from dataclasses_json.api import DataClassJsonMixin
from dataclasses_json import dataclass_json
from marshmallow.fields import DateTime
from log import configure_log
import jsonl
from blobstore import BlobStore
from pathlib import Path, PurePath
from typing import Callable, Iterable, List, Optional, TypeVar
from snowflake.connector.cursor import SnowflakeCursor
import spacy
from sf import sf_connect
from cfg import load_cfg
import asyncio
from dataclasses import dataclass


async def video_entities(args: Args):
    '''loads video named entities from a list of video id's in a jsonl.gz file'''
    cfg = await load_cfg()
    log = configure_log(cfg)
    if(cfg.state.videoPaths is None and args.videos is None):
        raise Exception('Need either videoPaths or videos')

    log.info('video_entities - test')
    log.info('video_entities - {machine} started: {state}', machine=cfg.machine, state=cfg.state.to_json())


asyncio.run(video_entities(load_args()))
