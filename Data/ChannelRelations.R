library("dplyr", lib.loc="~/R/win-library/3.5")



#channels <- read.csv("2.Analysis\\Channels.csv", header = TRUE)
outChannels <- Channels %>%
  filter(Status!="Default") %>% 
  select(Id, Title, Status, SubCount, ChannelVideoViews, Type, LR) %>%
  arrange(desc(ChannelVideoViews))



channelVideos <- Visits %>%
  group_by(FromChannelId) %>%
  summarise(ChannelRecommends = n())

#recommends <- read.csv("2.Analysis\\Visits.csv", header = TRUE)
outRelations <- Visits %>%
  group_by(FromChannelTitle, ChannelTitle, FromChannelId, ChannelId) %>%
  subset(ChannelId %in% outChannels$Id) %>% 
  subset(FromChannelId %in% outChannels$Id) %>%
  inner_join(channelVideos, by = "FromChannelId") %>%
  summarise(Recommends = n(), RecommendedViews = sum(FromVideoViews), RecommendsPerVideo=n()/mean(ChannelRecommends)) %>%
  filter(RecommendsPerVideo >= 0.02) %>%
  arrange(desc(Recommends))

write.csv(outChannels,"Channels.csv")
write.csv(outRelations,"ChannelRelations.csv")


