library("dplyr", lib.loc="~/R/win-library/3.5")

inDir <- "C:\\Users\\mark\\AppData\\Local\\YoutubeNetworks\\Data\\2018-11-03_11-36\\"
outDir <- "C:\\Users\\mark\\Repos\\YouTubeNetworks\\Site\\static\\data"

setwd(inDir)
channels <- read.csv("Channels.csv", header = TRUE)
outChannels <- channels %>%
  filter(Status=="Seed") %>% 
  select(Id, Title, Status, SubCount, ChannelVideoViews, Type, LR) %>%
  arrange(desc(ChannelVideoViews))



recommends <- read.csv("Visits.csv", header = TRUE)

channelVideos <- recommends %>%
  group_by(FromChannelId) %>%
  summarise(ChannelRecommends = n())

outRelations <- recommends %>%
  group_by(FromChannelTitle, ChannelTitle, FromChannelId, ChannelId) %>%
  subset(ChannelId %in% outChannels$Id) %>% 
  subset(FromChannelId %in% outChannels$Id) %>%
  inner_join(channelVideos, by = "FromChannelId") %>%
  summarise(Recommends = n(), RecommendedViews = sum(FromVideoViews), RecommendsPerVideo=n()/mean(ChannelRecommends)) %>%
  filter(RecommendsPerVideo >= 0) %>%
  arrange(desc(Recommends))

setwd(outDir)
write.csv(outChannels, "Channels.csv")
write.csv(outRelations,"ChannelRelations.csv")


