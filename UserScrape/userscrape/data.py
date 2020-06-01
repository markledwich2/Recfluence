import pandas as pd
from .cfg import UserCfg
from .store import BlobStore, BlobPaths
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Iterable, Any, Optional, Callable
from pandas.core.series import Series
from pathlib import PurePosixPath


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


class UserScrapeData:
    def __init__(self, store: BlobStore, trail_id: str):
        self.store = store
        self.path = self.path = BlobPaths(store.cfg, trail_id)

    def __load_res_csv(self, name: str, load_data: Callable[[pd.DataFrame], None]):
        """loads a results csv file form the trail run directory if it exists, otherwise from the latest recfluence output
        """
        file_name = PurePosixPath(f'{name}.csv.gz')
        in_path = self.path.results_path_in() / file_name
        if(not self.store.exists(in_path)):
            # copy to in then load it
            local_path = self.path.local_temp_path(file_name)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            blob_path = self.path.results_path_recfluence() / file_name
            self.store.load_file(local_path, blob_path)
            self.store.save_file(local_path, in_path)
        url = self.store.url(in_path)
        df = pd.read_csv(url)
        return load_data(df)

    def test_videos(self, num: Optional[int]) -> List[BasicVideo]:
        """gets the top 5 videos in the last 7 days for each ideology (distinct channels)"""
        # this SQL generates this file https://github.com/markledwich2/YouTubeNetworks_Dataform/blob/master/sql/user_scrape_tests.sql
        # I would like to use snowflake directly, but the python client is incompatible with azure blob storage client v12.
        # https://github.com/snowflakedb/snowflake-connector-python/issues/314
        # for the moment. Recfluence iwll write out the data we need to results each run.
        # for a trail copy that data so that it is the same every run
        def ld(df: pd.DataFrame):
            test_videos = [
                BasicVideo(row.VIDEO_ID, row.VIDEO_TITLE, row.CHANNEL_ID, row.CHANNEL_TITLE, row.IDEOLOGY)
                for i, row in df.iterrows()
            ]
            return test_videos if num == None else test_videos[0:num]
        return self.__load_res_csv('us_tests', ld)

    def seed_videos(self, num: int) -> Dict[str, List[SeedVideo]]:
        """a list of videos to seed user history with. Random videos proportional to views spread across channels in each ideology group

        Keyword Arguments:
            num {int} -- the number of videos to return per-ideology (default: {50})

        Returns:
            Dict[str, List[SeedVideo]] -- the seed videos grouped by ideology
        """

        def ld(df: pd.DataFrame):
            def videos(rows: Iterable[Any]):
                return [
                    SeedVideo(row.VIDEO_ID, row.VIDEO_TITLE, row.CHANNEL_ID,
                              row.CHANNEL_TITLE, row.IDEOLOGY, row.IDEOLOGY_RANK)
                    for i, row in rows
                ][0:num]

            # this SQL generates this file https://github.com/markledwich2/YouTubeNetworks_Dataform/blob/master/sql/user_scape_seeds.sql
            # todo: this list should be queries directly form snowflake. cheaper during development to start like this
            d = df.groupby(['IDEOLOGY']).apply(lambda g: videos(g.iterrows())).to_dict()
            return d
        return self.__load_res_csv('us_seeds', ld)
