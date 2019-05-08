# Political YouTube

A [visualization](https://pyt.azureedge.net) showing the relations and recommendation flows between political & cultural commentary on YouTube

## Updates
**14 Jan 2019**: New channels and some updates. The network diagram looks quite different at first, but that's mostly cosmetic. The rotation and location of them are somewhat different but it clusters similarly. To see the older version matching the published article use [this link](https://pyt.azureedge.net?v=2018-12-28)

## Data Collection Process 

### How channels are selected
Channels were included if the met the following criteria
- 10k+ subscribers. If subscriber data is missing/lower, still include if video's average above 10k views
- Significant focus (more than 30% of content) on US political or cultural news/commentary. I considered cultural commentary was anything from the [ISideWith social issues list](https://www.isidewith.com/en-us/polls)

There is no definitive list of YouTube channels, so a variety of techniques were used. 
- Reviewed the following lists:
    - https://www.adfontesmedia.com/
    - https://blog.feedspot.com/political_youtube_channels/
    - https://socialblade.com/youtube/top/tag/politics/videoviews
    - https://channelcrawler.com/
    - custom algorithm prioritizing most recommended channels from existing seed channels. [ChannelExclude.csv](Data/ChannelExclude.csv) is was used to remove recommendations once reviewed.
    - Data & Society [Alternative Influence Report](https://datasociety.net/output/alternative-influence/)
- Related videos/channels from existing channels
- Searches for keywords from [ISideWith social issues list](https://www.isidewith.com/en-us/polls)
- Suggestions from reviewers and my own knowledge

Over time, the ease of finding channels has diminished. I estimate the list as of 2018-12-13 is at least 2/3 of all channels that meet this criteria (in terms of views). Please [email me](mailto:mark@ledwich.com.au) if you have additional channel suggestions.

### How channels political category was determined
For news:
- Compare https://www.adfontesmedia.com and https://mediabiasfactcheck.com/. If they exist in those lists and were in agreement then I accepted that category. Otherwise I used the same process as with the commentary channels.

For political/cultural commentary I considered all of the following:
 - Self identified political label, or support for a party
 - One sided content on divided political topics of the day (e.g. Kavanaugh, Migrant Caravan)
 - One sided content reacting to cultural events topics of the day (e.g. campus protests, trans activism )
 - Clearly matches Democrat (left) or Republican (right) views on issues in the [ISideWith poll](https://www.isidewith.com/en-us/polls)

 If these considerations align in the same direction then the channel is left or right. If there was a mix then they are assigned the center/heterodox category.

**Political Category FAQ**
- The classification of political category is just one persons subjective opinion, how can you trust the results when it is so subjective?  There is some merit to this, you can't trust it as much as if I had manage to get all YouTubers to take a survey about their attitudes/content for example. It doesn't need to be a reason to dismiss my analysis for the following reasons:
    - I used respected sources of classification where possible (i.e. adfontesmedia.com and mediabiasfactcheck.com) which covered a large portion of the large mainstream channels.
    - The top 50 channels by video views make up 78% of all views. Download the channel data, then Go though the top 50 channels and check if you agree with the classification. If you generally accept the classifications then you can generally accept the results.

- Why not perform a more procedural/quantifiable method for determining political category (e.g. like the process used for https://www.adfontesmedia.com)? I don't believe making this process more detailed and quantitative would give you a significantly more accurate/objective answer. .
- Why is the apposition to Identity Politics/Social Justice considered "right" when it is not normally considered an important part of the standard political definition for left/right?  I understand this, but I am confident this has changed. It is clear when evaluating YouTube content that this is a new and important divide. 
- The left/right dichotomy is not a good way to classify tribal politics, why do it that way?. I agree, and forcing it into this model creates many needless problems because it is not a natural category for this data. I was forced to use left/right because I wanted to use this data to evaluate the common narratives about YouTube radicalization which was already framed with this dichotomy.

### How recommended video's are retrieved
The YouTube API is used to get all channel, video, recommendation data.
For all seed channels, get a list of videos within the configured time range (Jan 1 2018 or later)
For all videos, retrieve the top 10 recommended videos


### Download Data

**CSV**

Updated 4th April 2019

[Channels](https://ytnetworks.azureedge.net/data/results/2019-04-04/VisChannels.csv)

[Channel Relations](https://ytnetworks.azureedge.net/data/results/2019-04-04/VisRelations.csv)

[Daily Video Recommendations](https://ytnetworks.azureedge.net/data/results/2019-04-04/DailyVideoRecommends.zip)


**Connect Directly to Azure Storage**

Example [Databricks notebook](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5467014801025226/3416558578232351/7194280856364978/latest.html)
connecting to azure storage (public read access)

## Visualization Information

**Channel Relation Graph**
The "bubble" chart is a [force directed graph](https://en.wikipedia.org/wiki/Force-directed_graph_drawing). The area (not the radius) of each bubble corresponds to the number of views of a channels video's. The force/size of link line corresponds to the portion of recommendations between those channels.

NOTE:
- When new data is added, the orientation and the final clustering of channels changes significantly. Unlike a principal component analysis (or similar) it does not display a quantifiable statistic by the location. The chart animates the simulated forces when opening to show this process.


**Recommendation Flow Diagram**
The left boxes in the flow diagram show the number of views. it is broken down by split by channel (when one is selected) or political category otherwise. The right side shows the portion of those views (according to the number of times the other channels video's were in the recommended list) given to the category/channel. 


## Implementation
### Data Analysis

[A function](App/YtFunctions/YtFunctions.cs) runs each day and updates data from the YouTube API about the seed channels, their videos and recommendations.
- Cached in cloud storage with history
- Collected into a snapshot data as of each day into .parquet files in azure storage
- Analysed using a databricks notebook into csv files (as listed in download data)


 
