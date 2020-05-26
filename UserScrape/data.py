import pandas as pd
from cfg import UserCfg
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Iterable, Any
from pandas.core.series import Series


@dataclass
class BasicVideo:
    video_id: str
    video_title: str
    channel_id: str
    channel_title: str
    ideology: str


@dataclass
class SeedVideo(BasicVideo):
    ideology_rank: int

# todo: this list should be queries directly form snowflake. cheaper during development to start like this


def load_test_videos() -> List[BasicVideo]:
    """gets the top 5 videos in the last 7 days for each ideology (distinct channels)"""
    # this SQL generates this file https://github.com/markledwich2/YouTubeNetworks_Dataform/blob/master/sql/user_scrape_tests.sql
    # todo: this list should be queries directly form snowflake. cheaper during development to start like this
    df = pd.read_csv('https://pyt.blob.core.windows.net/data/results/user_scrape/video_seeds.csv.gz')
    test_videos = [
        BasicVideo(row.VIDEO_ID, row.VIDEO_TITLE, row.CHANNEL_ID, row.CHANNEL_TITLE, row.IDEOLOGY)
        for i, row in df.iterrows()
    ]
    return test_videos


def load_seed_videos(num: int = 50) -> Dict[str, List[SeedVideo]]:
    """a list of videos to seed user history with. Random videos proportional to views spread across channels in each ideology group

    Keyword Arguments:
        num {int} -- the number of videos to return per-ideology (default: {50})

    Returns:
        Dict[str, List[SeedVideo]] -- the seed videos grouped by ideology
    """

    def videos(rows: Iterable[Any]):
        return [
            SeedVideo(row.VIDEO_ID, row.VIDEO_TITLE, row.CHANNEL_ID, row.CHANNEL_TITLE, row.IDEOLOGY, row.IDEOLOGY_RANK)
            for i, row in rows
        ][0:num]

    # this SQL generates this file https://github.com/markledwich2/YouTubeNetworks_Dataform/blob/master/sql/user_scape_seeds.sql
    # todo: this list should be queries directly form snowflake. cheaper during development to start like this
    df = pd.read_csv('https://pyt.blob.core.windows.net/data/results/user_scrape/video_seeds.csv.gz')
    d = df.groupby(['IDEOLOGY']).apply(lambda g: videos(g.iterrows())).to_dict()
    return d
