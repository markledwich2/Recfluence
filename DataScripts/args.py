import argparse
from typing import List
from dataclasses import dataclass


@dataclass
class Args:
    """ A list"""
    videos: List[str]


def load_args():
    parser = argparse.ArgumentParser(description='Run python scripts against the recfluence/ttube warehouse')
    parser.add_argument("--videos", "-v",
                        help="A comma separeted list fo videos to perform named entity recognition on. e.g. So1i3LA6IIU,CL__k9QzfyE. If not specified, videos batch files need to be in the run_state environment variable (the way other application provide a list of video_id's).",
                        default=None)
    args = parser.parse_args()

    return Args(list(args.videos.split(',')) if args.videos else None)
