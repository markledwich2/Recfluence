
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
import tempfile
import time
import secrets


@dataclass
class DbVideoEntity:
    videoId: str
    videoTitle: str
    descripton: Optional[str]
    captions: Optional[str]
    videoUpdated: DateTime
    captionUpdated: Optional[DateTime]


@dataclass_json
@dataclass
class DbCaption(DataClassJsonMixin):
    offset: Optional[int] = None
    caption: Optional[str] = None


@dataclass
class VideoCaption:
    video: DbVideoEntity
    offset: Optional[int] = None
    caption: Optional[str] = None


@dataclass
class Entity:
    name: str
    type: str


@dataclass
class VideoEntity:
    videoId: str
    part: str  # title, description, captions
    offset: Optional[int]
    entities: List[Entity]
    videoUpdated: DateTime
    captionUpdated: Optional[DateTime]
    updated: DateTime


BATCH = 2000


def get_ents(pipe_res) -> List[Entity]:
    return list(map(lambda r: list([Entity(ent.text.strip(), ent.label_) for ent in r.ents]), pipe_res))


T = TypeVar('T')

EXCLUDE_LABELS = ['CARDINAL', 'MONEY', 'DATE']


async def video_entities():
    '''loads video named entities from a list of video id's in a jsonl.gz file'''
    cfg = await load_cfg()
    log = configure_log(cfg.seq.seqUrl, branchEnv=cfg.branchEnv)
    log.info('video_entities - started')
    blob = BlobStore(cfg.storage)
    space_lg = spacy.load("en_core_web_sm", disable=['parser', 'tagger', 'textcat', 'lemmatizer'])

    localPath = Path(tempfile.gettempdir()) / 'data_scripts' / 'video_entities'
    localPath.mkdir(parents=True, exist_ok=True)

    db = sf_connect(cfg.snowflake)
    try:
        cur: SnowflakeCursor = db.cursor()
        sql = f'''

with
  load as (
    {f'select $1:video_id::string video_id from @public.yt_data/{cfg.videoPath}' if cfg.videoPath 
    else 'select video_id from video_latest limit 100'}
)
  , vids as (
  select v.video_id, v.video_title, v.description, v.updated
  from load l
  join video_latest v on v.video_id = l.video_id
  where not exists(select * from video_entity e where e.video_id = v.video_id)
  order by video_id
)
, s as (
  select v.video_id
     , any_value(video_title) video_title
     , any_value(description) description
     , array_agg(object_construct('offset',s.offset_seconds::int,'caption',s.caption)) within group ( order by offset_seconds ) captions
      , max(v.updated) video_updated
      , max(s.updated) caption_updated
from vids v
    left join caption s on v.video_id=s.video_id
    group by v.video_id
)
select * from s
        '''

        log.info('video_entities - getting data for this video batch: {sql}', sql=sql)
        cur.execute(sql)

        log.info('video_entities - processing entities')

        def entities(rows: List[T], getVal: Callable[[T], str]) -> Iterable[Iterable[Entity]]:
            res = list(space_lg.pipe([getVal(r) or "" for r in rows], n_process=4))
            return map(lambda r: [Entity(ent.text.strip(), ent.label_) for ent in r.ents if ent.label_ not in EXCLUDE_LABELS], res)

        def captions(json) -> List[DbCaption]:
            return DbCaption.schema().loads(json, many=True)

        while True:
            raw_rows = cur.fetchmany(BATCH)
            if(len(raw_rows) == 0):
                break
            source_videos = list(map(lambda r: DbVideoEntity(r[0], r[1], r[2], r[3], r[4], r[5]), raw_rows))
            title_entities = entities(source_videos, lambda r: r.videoTitle)
            description_entities = entities(source_videos, lambda r: r.descripton)
            source_captions = [
                VideoCaption(r, c.offset, c.caption) for r in source_videos
                for c in captions(r.captions)
            ]
            caption_entities = entities(source_captions, lambda r: r.caption)

            updated = datetime.now(timezone.utc)
            caption_rows = map(lambda r, t: VideoEntity(r.video.videoId, 'caption', r.offset, t, r.video.videoUpdated,
                                                        r.video.captionUpdated, updated), source_captions, caption_entities)
            res_rows = list(chain(map(lambda r, t: VideoEntity(r.videoId, 'title', None, t, r.videoUpdated, r.captionUpdated, updated), source_videos, title_entities),
                                  map(lambda r, t: VideoEntity(r.videoId, 'description', None, t, r.videoUpdated,
                                                               r.captionUpdated, updated), source_videos, description_entities),
                                  [r for r in caption_rows if r.offset is not None or r.entities is not None]))

            fileName = f'{time.strftime("%Y-%m-%d_%H-%M-%S")}.{secrets.token_hex(5)[:5]}.jsonl.gz'
            localFile = localPath / fileName
            with gzip.open(localFile, 'wb') as f:
                jsonl.dump(res_rows, f, cls=jsonl.JsonlEncoder)
            blob.save_file(localFile, PurePath(f'db2/video_entities/{fileName}'))
            log.info('video_entities - saved {rows} video entities into {fileName}', rows=len(res_rows), fileName=fileName)

    finally:
        cur.close()
        db.close()

asyncio.run(video_entities())
