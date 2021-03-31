from logging import Logger
import tempfile
import time
import secrets
import re

import spacy
from spacy.language import Language
from args import Args
from datetime import datetime, timezone
import gzip
from itertools import chain
from dataclasses_json.api import DataClassJsonMixin
from dataclasses_json import dataclass_json
import jsonl
from blobstore import BlobStore
from pathlib import Path, PurePath
from typing import Any, Callable, Iterable, List, Optional, TypeVar, Union, cast
from snowflake.connector.cursor import DictCursor, SnowflakeCursor
from sf import sf_connect
from cfg import Cfg, Part, should_run_part
from dataclasses import dataclass


@dataclass
class DbVideoEntity:
    videoId: str
    videoTitle: str
    descripton: Optional[str]
    captions: Optional[str]
    videoUpdated: datetime


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
    start_char: int
    end_char: int


@dataclass
class VideoEntity:
    videoId: str
    part: str  # title, description, captions
    offset: Optional[int]
    entities: List[Entity]
    videoUpdated: Optional[datetime] = None
    updated: Optional[datetime] = None


T = TypeVar('T')

EXCLUDE_LABELS = ['CARDINAL', 'MONEY', 'PERCENT', 'ORDINAL']


def clean_text(text):
    # Remove newlines and lowercase if mostly uppercase (both first letter or all letters)
    text = re.sub(r'\s+', ' ', text)
    if len(text) < 10:
        return text
    if sum(1 for x in text if x.islower()) == 0:
        return text.lower()
    tok_l = text.split(" ")
    if len(tok_l) >= 5 and sum(1 for tok in tok_l if len(tok) > 0 and tok[0].islower()) == 0:
        return text.lower()
    return text


def get_entities(lang: Language, rows: List[T], getVal: Callable[[T], Union[str, None]] = None) -> Iterable[Iterable[Entity]]:
    def get_cleaned_txt(r: T):
        val = (getVal(r) if getVal is not None else r) or ""
        return clean_text(val)

    res: List[Any] = list(lang.pipe([get_cleaned_txt(r) for r in rows], n_process=4))
    return map(lambda r: [Entity(e.text.strip(), e.label_, e.start_char, e.end_char)
                          for e in r.ents if e.label_ not in EXCLUDE_LABELS], res)


def get_language():
    return spacy.load("en_core_web_sm", disable=['parser', 'tagger', 'textcat', 'lemmatizer'])


def videos_to_entities(source_videos: List[DbVideoEntity], lang: Language, parts: Optional[List[Part]] = None):

    def entities(rows: List[T], part: Part, getVal: Callable[[T], Union[str, None]]):
        return get_entities(lang, rows, getVal) if should_run_part(parts, part) else []

    def captions(json) -> List[DbCaption]:
        return cast(List[DbCaption], DbCaption.schema().loads(json, many=True)) if json else []

    title_entities = entities(source_videos, Part.title, lambda r: r.videoTitle)
    description_entities = entities(source_videos, Part.description, lambda r: r.descripton)
    source_captions = [
        VideoCaption(r, c.offset, c.caption) for r in source_videos
        for c in captions(r.captions)
    ] if should_run_part(parts, Part.caption) else []
    caption_entities = entities(source_captions, Part.caption, lambda r: r.caption)

    updated = datetime.now(timezone.utc)
    caption_rows = map(lambda r, t: VideoEntity(r.video.videoId, 'caption', r.offset, list(t),
                       r.video.videoUpdated, updated), source_captions, caption_entities)
    res_rows = list(chain(map(lambda r, t: VideoEntity(r.videoId, 'title', None, t, r.videoUpdated, updated), source_videos, title_entities),
                          map(lambda r, t: VideoEntity(r.videoId, 'description', None, t, r.videoUpdated, updated), source_videos, description_entities),
                          [r for r in caption_rows if r.offset is not None or r.entities is not None]))
    return res_rows


def video_entities(cfg: Cfg, args: Args, log: Logger):
    blob = BlobStore(cfg.storage)
    parts = cfg.state.parts

    localBasePath = Path(tempfile.gettempdir()) / 'data_scripts' if cfg.localDir is None else Path(cfg.localDir)
    localPath = localBasePath / 'video_entities'
    localPath.mkdir(parents=True, exist_ok=True)

    db = sf_connect(cfg.snowflake)
    try:

        videoSql = ','.join([f"'{v}'" for v in args.videos]) if args.videos else None

        selects = list([f'select $1:video_id::string video_id from @public.yt_data/{p}' for p in cfg.state.videoPaths]) \
            if cfg.state.videoPaths else list([f'select video_id from video_latest where video_id in ({videoSql})'])

        batchTotal = len(selects)
        batchNum = 0
        for select in selects:
            batchNum = batchNum + 1

            cur: DictCursor = db.cursor(DictCursor)
            sql = f'''

with load as ({select})
  , vid as (
  select v.video_id, v.video_title, v.description, v.updated video_updated
  from load l
         join video_latest v on v.video_id=l.video_id
  order by video_id
)
  , vid_caption as (
  select v.video_id
       , any_value(video_title) video_title
       , any_value(description) description
       , array_agg(object_construct('offset',s.offset_seconds::int,'caption',s.caption)) within group ( order by offset_seconds ) captions
       , max(v.video_updated) video_updated
       , max(s.updated) caption_updated
  from vid v
         left join caption s on v.video_id=s.video_id
  group by v.video_id
)
select *
from {'vid_caption' if should_run_part(parts, Part.caption) else 'vid'}
            '''

            log.info('video_entities - getting data for this video file batch {batch}/{batchTotal}: {sql}',
                     sql=sql, batch=batchNum, batchTotal=batchTotal)
            sqlRes = cast(DictCursor, cur.execute(sql))
            videoTotal = sqlRes.rowcount
            log.debug('video_entities - batch sql complete. About to process entities')

            lang = get_language()
            videoCount = 0
            while True:
                raw_rows = cur.fetchmany(cfg.dataScripts.spacyBatchSize)
                if(len(raw_rows) == 0):
                    break
                source_videos = list(map(lambda r: DbVideoEntity(r['VIDEO_ID'], r['VIDEO_TITLE'], r['DESCRIPTION'],
                                     r.get('CAPTIONS'), r.get('VIDEO_UPDATED')), raw_rows))
                videoCount = videoCount + len(raw_rows)
                res_rows = videos_to_entities(source_videos, lang, parts)

                fileName = f'{time.strftime("%Y-%m-%d_%H-%M-%S")}.{secrets.token_hex(5)[:5]}.jsonl.gz'
                localFile = localPath / fileName
                blobFile = PurePath(f'db2/video_entities/{fileName}') if cfg.localDir is None else None
                with gzip.open(localFile, 'wb') as f:
                    jsonl.write_jsonl(res_rows, f)
                if(blobFile):
                    blob.save_file(localFile, blobFile)

                log.info('video_entities - saved {file} {videoCount}/{videoTotal} videos in batch {batch}/{batchTotal}',
                         videoCount=videoCount, videoTotal=videoTotal, file=str(blobFile) if blobFile else str(localFile),
                         batch=batchNum, batchTotal=batchTotal)

            cur.close()

    finally:
        db.close()

    log.info('video_entities - cometed successfully')
