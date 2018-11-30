# Political YouTube

A visualization showing the relations and recommendaton flows between politcal & cultural commantary on youtube

## Data

Download latest data 
[Channels.csv](https://ytnetworks.blob.core.windows.net/data/results/VisChannels.csv)
and [Relations.csv](https://ytnetworks.blob.core.windows.net/data/results/VisRelations.csv)

### Source Data
[SeedChannels.csv](Data/SeedChannels.csv) contains a list of channels to include in the anaysis. The LR column is hand-populated using https://mediabiasfactcheck.com/ and judgement by reviewing the contents of their channel. 

Initially populated using the following criterial: 1k+ subs & significant focus on US political/cultural commentary/contexualizing.

- https://blog.feedspot.com/political_youtube_channels/
- https://socialblade.com/youtube/top/tag/politics/videoviews
- https://channelcrawler.com/
- 2018 viewcount search for Democrats|Republicans.
- custom algorithm prioritizing most recommended channels from exisitng seed channels. [ChannelExclude.csv](Data/ChannelExclude.csv) is was used to remove recommendations once reviewed.
- data & society report
- additional suggestions

### Videos, Recommendations, Channel Statistics

[A function](App/YtFunctions/YtFunctions.cs) runs each day and updates data fromt he YouTube API about the seed channels, their videos and recommendaitons.
- Cached in cloud storage with history
- Collected into a snapshot data as of each day into Channels.parquet , Videos.parquet , Recommends.parquet files publically accesssable (replace date and file as required): https://ytnetworks.blob.core.windows.net/data/analysis/2018-11-28/Channels.parquet
- Data from 2018 Jan 1st - Now
- Analysed using [this databricks notebook](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5467014801025226/1340434901968186/7194280856364978/latest.html) which contains descriptions of the process

## Implimentation

Stack: Gatsby, React, D3 hosted on an Azure Static Website.

- Build: Automated. On checking, azure Pipeines will build App & Site. See https://dev.azure.com/mledwich/ytnetworks. 
- Release: Create a release in Azure pipelines to deploy
- Update Data: 
    - Run [ChannelRelations notebook](https://community.cloud.databricks.com/?o=5467014801025226#notebook/1340434901968186/command/1340434901968187) the final cell saves the data used by the visualization