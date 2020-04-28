import pandas as pd
from cfg import UserCfg
from dataclasses import dataclass, field
from typing import List

@dataclass
class SeedVideo:
    video_id:str
    video_title:str
    channel_id: str
    channel_title: str
    ideology:str
    ideology_rank:int

    

def load_all_seeds() -> pd.DataFrame:
    # this SQL generates this file https://github.com/markledwich2/YouTubeNetworks_Dataform/blob/master/sql/user_scrape_videos_to_watch.sql
    all_seeds = pd.read_csv('https://pyt.blob.core.windows.net/data/results/user_scrape/video_seeds.csv.gz')
    return all_seeds

def seeds_for_user(user:UserCfg, all_seeds:pd.DataFrame, num:int=50):
    user_df = all_seeds[all_seeds["IDEOLOGY"] == user.ideology]
    user_videos = [SeedVideo(row.VIDEO_ID, row.VIDEO_TITLE, row.CHANNEL_ID, row.CHANNEL_TITLE, row.IDEOLOGY, row.IDEOLOGY_RANK) for i, row in user_df.iterrows() ]
    return user_videos






    