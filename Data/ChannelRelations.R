library("dplyr", lib.loc="~/R/win-library/3.5")



channels <- read.csv("2.Analysis\\Channels.csv", header = TRUE)
outChannels <- select(filter(channels, Status!="Ignored"), Id, Title, Status, SubCount)
write.csv(outChannels,".\\3.Vis\\Channels.csv")

recommends <- read.csv("2.Analysis\\Visits.csv", header = TRUE)
outRelations <- recommends %>%
  group_by(FromChannelTitle, ChannelTitle, FromChannelId, ChannelId) %>%
  subset(ChannelId %in% outChannels$Id) %>% 
  subset(FromChannelId %in% outChannels$Id) %>%
  summarise(Value = n()) %>%
  arrange(desc(Value))


write.csv(outRelations,".\\3.Vis\\ChannelRelations2.csv")
