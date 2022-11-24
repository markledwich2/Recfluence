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
- video rec stats for NYT comparison

| ACCOUNT | RECS | INTRA\_CATEGORY | SELF\_RECS | CHANNEL\_WATCHED\_RECS | NOVEL\_RECS | PCT\_LEFT | PCT\_RIGHT |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| PartisanLeft | 153624 | 0.370919 | 0.145296 | 0.208327 | 0.658953 | 0.801490 | 0.198510 |
| AntiSJW | 175028 | 0.150010 | 0.145657 | 0.063778 | 0.791171 | 0.434597 | 0.565403 |
| AntiTheist | 164691 | 0.111020 | 0.180064 | 0.091153 | 0.727362 | 0.725231 | 0.274769 |
| Mainstream News | 142358 | 0.382241 | 0.142050 | 0.152650 | 0.712907 | 0.647747 | 0.352253 |
| SocialJustice | 158548 | 0.204304 | 0.138538 | 0.137813 | 0.733809 | 0.780988 | 0.219012 |
| LateNightTalkShow | 150467 | 0.339257 | 0.131318 | 0.422930 | 0.458865 | 0.901812 | 0.098188 |
| WhiteIdentitarian | 160529 | 0.031265 | 0.170586 | 0.049337 | 0.777212 | 0.478018 | 0.521982 |
| Conspiracy | 161158 | 0.033352 | 0.175430 | 0.019788 | 0.802734 | 0.500732 | 0.499268 |
| PartisanRight | 163516 | 0.222810 | 0.168253 | 0.071180 | 0.754110 | 0.419954 | 0.580046 |
| Libertarian | 168139 | 0.084109 | 0.156061 | 0.084210 | 0.760157 | 0.403043 | 0.596957 |
| MRA | 167365 | 0.095982 | 0.177976 | 0.077770 | 0.736677 | 0.407203 | 0.592797 |
| QAnon | 169849 | 0.005428 | 0.195291 | 0.005835 | 0.795942 | 0.512000 | 0.488000 |
| Socialist | 156044 | 0.100747 | 0.168799 | 0.083175 | 0.746578 | 0.769786 | 0.230214 |
| ReligiousConservative | 153693 | 0.106557 | 0.171719 | 0.082710 | 0.743040 | 0.411588 | 0.588412 |
| Fresh | 179136 | 0.000000 | 0.336281 | 0.000000 | 0.660241 | 0.613344 | 0.386656 |

- home page rec stats

| ACCOUNT | INTRA\_CATEGORY | PCT\_CHANNEL\_WATCHED | PCT\_LEFT | PCT\_RIGHT |
| :--- | :--- | :--- | :--- | :--- |
| AntiSJW | 0.470810 | 0.370214 | 0.042361 | 0.957639 |
| AntiTheist | 0.420997 | 0.400556 | 0.801683 | 0.198317 |
| Conspiracy | 0.173315 | 0.154234 | 0.075013 | 0.924987 |
| LateNightTalkShow | 0.353000 | 0.430091 | 0.984592 | 0.015408 |
| Libertarian | 0.320670 | 0.379543 | 0.066773 | 0.933227 |
| MRA | 0.418442 | 0.417632 | 0.038504 | 0.961496 |
| Mainstream News | 0.394907 | 0.346057 | 0.723444 | 0.276556 |
| PartisanLeft | 0.500242 | 0.417535 | 0.978074 | 0.021926 |
| PartisanRight | 0.371573 | 0.267891 | 0.031642 | 0.968358 |
| QAnon | 0.127600 | 0.128495 | 0.063147 | 0.936853 |
| ReligiousConservative | 0.418783 | 0.370926 | 0.021305 | 0.978695 |
| SocialJustice | 0.443230 | 0.369041 | 0.962311 | 0.037689 |
| Socialist | 0.413104 | 0.422370 | 0.974376 | 0.025624 |
| WhiteIdentitarian | 0.147113 | 0.289856 | 0.060016 | 0.939984 |
