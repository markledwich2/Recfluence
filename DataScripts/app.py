
from video_entities import video_entities
from args import Args, load_args
from log import configure_log
from cfg import load_cfg
import asyncio


async def run(args: Args):
    '''loads video named entities from a list of video id's in a jsonl.gz file'''
    cfg = await load_cfg()
    log = configure_log(cfg)
    if(cfg.state.videoPaths is None and args.videos is None):
        raise Exception('Need either videoPaths or videos')

    log.info('video_entities - {machine} started: {state}', machine=cfg.machine, state=cfg.state.to_json())

    video_entities(cfg, args, log)

if __name__ == "__main__":
    asyncio.run(run(load_args()))
