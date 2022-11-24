# YouTube Personalized Recommendations
Fourteen Google accounts were created and assigned a persona to examine the influence of watch history on recommendations. A persona simulates a user with a watch history within one political video category (e.g., Partisan Left, Anti-Woke). To classify the channels, we trained a model that uses a channels common subscribers to predict a channel's categories. A detailed description of this classifier and its performance can be found [here](https://github.com/sam-clark/chan2vec).

Before starting our experiment, each account started from a clean slate. The experiment set-up was to build different ‘bubbled’ watch histories as input for the recommendation system, which collected the recommended videos for each account while the user was watching videos. An anonymous, neutral user who has not created a profile on the platform and who has no personalised and recorded history was used as a control to compare personalised recommendations with recommendations produced for a viewer with no watch history. This allowed us to identify how page and video recommendations are influenced by personalisation.    



During the collection, our political dataset was expanded to include ~7,000 channels.


The following steps were repeated each day from 7 September 2020 through 1 January 2021:
![image](https://user-images.githubusercontent.com/17095341/203696860-18b5eb32-713a-4900-920c-79503692a560.png)
- To build the persona’s history, videos from the last year were chosen randomly using a view-weighted sampling method. For example, the Manosphere persona watched videos from channels with the Manosphere tag. The random selection was weighted by video popularity (i.e., number of views) better to represent the watching behaviour of a typical user. This set was performed for 50 videos initially and five on each subsequent day.
- Each persona’s YouTube homepage was loaded 20 times in rapid succession once a day, starting at 1 am GMT and the recommended videos were recorded. For each video ‘viewed’, 20 recommendations were recorded. Each persona watched a view-weighted sample of political videos less than seven days old, and the ‘Up Next’ video recommendations were recorded. The personas were viewed on a cloud server container located on the West Coast of the United States to simulate an American viewer.
We used a common sample of videos for all personas each day to isolate the impact of watching history. The common sample consists of 100 videos, chosen randomly (proportional to views), published from political channels in the last seven days. They are ‘viewed’ by each persona. The common sample allows us to see what effect personalisation has on recommendations.
- Videos from the common set were not added to the watch history so as not to influence further recommendations. This was done by disabling the watch history for the personas. After each round of common sample viewing, recommendations were enabled again to build up the watch history for the next personalised set.


## Results
See [this article](https://transparency.tube/articles/personalization/) that summarizes our results. Note: a paper will be linked soon.


## Analysis Source


Here are the different parts used to analyse the data.  
- A [dataform project](https://github.com/markledwich2/YouTubeNetworks_Dataform) was used to transform the data 
- Some [ad-hoc analysis](https://github.com/markledwich2/YouTubeNetworks_Dataform/blob/master/sql/us_analyisis_scripts.sql)
- A [Tableau worksheet](https://public.tableau.com/app/profile/mark.ledwich/viz/PersonalizedRecommendationsv2/RecsvAnon)
- Javascript for the article viz. [Entry point page](https://github.com/markledwich2/TransparencyTube/blob/master/src/pages/articles/personalization.tsx)

## Data
- [personalization recommendation statistics](https://ytapp.blob.core.windows.net/public/results%2Fus_rec_stats_v2.jsonl.gz)
- [channels](https://ytapp.blob.core.windows.net/public/results%2Fttube_channels.jsonl.gz)
