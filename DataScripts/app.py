
from typing import Any, Iterable, List, Tuple
from snowflake.connector.cursor import SnowflakeCursor
import spacy
from ner import ner_run
from sf import sf_connect, sf_test
from cfg import load_cfg
import asyncio
from dataclasses import dataclass


@dataclass
class Entity:
    name: str
    type: str


@dataclass
class VideoEntity:
    video_id: str
    title_entities: List[Entity]
    description_entities: List[Entity]


BATCH = 1000


def get_ents(pipe_res) -> List[Entity]:
    return list(map(lambda r: list([Entity(ent.text.strip(), ent.label_) for ent in r.ents]), pipe_res))


async def run():
    sp_lg = spacy.load("en_core_web_sm", disable=[
                       'parser', 'tagger', 'textcat', 'lemmatizer'])

    cfg = await load_cfg()
    db = sf_connect(cfg.snowflake)
    try:
        cur: SnowflakeCursor = db.cursor()
        cur.execute_async(
            'select video_id, video_title, description from video_latest limit 2000')
        query_id = cur.sfqid
        cur.get_results_from_sfqid(query_id)

        while db.is_still_running:
            rows = cur.fetchmany(1000)
            title_entities = get_ents(sp_lg.pipe([r[1] or "" for r in rows]))
            description_entities = get_ents(
                sp_lg.pipe([r[2] or "" for r in rows]))
            store_rows = list(map(lambda r, t, d: VideoEntity(
                r[0], t, d), rows, title_entities, description_entities))
            # todo merge back with id's and save into blob storage
            print(f'loaded {store_rows.count()}')
    finally:
        db.close()

asyncio.run(run())
